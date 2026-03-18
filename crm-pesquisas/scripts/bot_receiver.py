"""
bot_receiver.py — Bot de registro de clientes via Telegram

Responsabilidade única: capturar o telegram_chat_id de cada cliente
e persistir em clientes.telegram_chat_id no Supabase.

Fluxo do cliente:
  Mobile  → clica em START → botão "Compartilhar meu contato" → contato nativo
  Desktop → clica em START → digita o número manualmente → bot valida e registra

  O botão nativo request_contact é exclusivo do app mobile do Telegram.
  No Telegram Web e Desktop o botão não é renderizado — por isso o bot
  aceita o número digitado como texto como fallback automático.

Sincronicidade com os outros scripts:
  - file_ingestion.py : normalizacao de telefone identica (re.sub(r'\\D', '', tel))
  - message_sender.py : le clientes.telegram_chat_id; este bot o escreve
  - Ambos usam o mesmo TELEGRAM_BOT_TOKEN e cliente Supabase

Instalacao:
    pip install "python-telegram-bot[job-queue]" supabase python-dotenv

Execucao:
    python bot_receiver.py
"""

import os
import re
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from message_sender import disparar_pesquisas as _disparar_pesquisas, reenviar_pesquisas_pendentes

from telegram import (
    Update,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Importação antecipada do sender para evitar import repetido dentro de threads.
# O try/except permite que bot_receiver rode mesmo sem message_sender presente.
try:
    from message_sender import disparar_pesquisas as _disparar_pesquisas
    _SENDER_DISPONIVEL = True
except ImportError:
    _disparar_pesquisas = None
    _SENDER_DISPONIVEL = False

# =============================================================================
# CONFIGURACAO
# =============================================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SUPABASE_URL       = os.getenv("SUPABASE_URL")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY")

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# --- Cliente Supabase ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase inicializado com sucesso.")
except Exception as e:
    logger.critical(f"Erro ao inicializar Supabase: {e}")
    supabase = None

# --- Executor para rodar tarefas sincronas sem bloquear o event loop do bot ---
# message_sender.disparar_pesquisas() é sincrono e usa asyncio.run() internamente.
# Dentro de um handler async já existe um loop ativo — ThreadPoolExecutor evita
# o RuntimeError que asyncio.run() levantaria nesse contexto.
_executor = ThreadPoolExecutor(max_workers=2)


# =============================================================================
# BLOCO 1 — HELPERS DE TELEFONE
# Mesma logica do file_ingestion.py — obrigatorio para garantir match correto.
# =============================================================================

def _normalizar_telefone(telefone: str) -> str:
    """Remove todos os caracteres nao numericos."""
    if not telefone:
        return ""
    return re.sub(r'\D', '', str(telefone))


def _variantes_telefone(telefone: str) -> list[str]:
    """
    Gera variantes do telefone para busca tolerante a DDI.

    O CSV pode ter armazenado '11999990001' (sem DDI) enquanto o Telegram
    retorna '+5511999990001'. Este helper cria ambas as formas para
    garantir o match independente do formato gravado pelo file_ingestion.py.

    Exemplos:
      '+5511999990001' -> ['5511999990001', '11999990001']
      '11999990001'    -> ['11999990001',   '5511999990001']
    """
    tel = _normalizar_telefone(telefone)
    variantes = [tel]

    if tel.startswith("55") and len(tel) > 11:
        variantes.append(tel[2:])       # 5511999... -> 11999...
    elif not tel.startswith("55") and len(tel) >= 10:
        variantes.append("55" + tel)    # 11999...   -> 5511999...

    return variantes


def _parece_telefone(texto: str) -> bool:
    """
    Retorna True se o texto digitado pelo usuario parece um numero de telefone.
    Aceita formatos como: 11999990001, (11) 99999-0001, +5511999990001, etc.
    Exige minimo de 10 digitos apos remocao de caracteres nao numericos.
    """
    apenas_digitos = _normalizar_telefone(texto)
    return len(apenas_digitos) >= 10


# =============================================================================
# BLOCO 2 — CONSULTAS AO BANCO
# =============================================================================

def _buscar_cliente_por_telefone(telefone: str) -> dict | None:
    """
    Busca o cliente em 'clientes' testando todas as variantes de telefone.
    Retorna o registro completo ou None se nao encontrado.
    """
    if not supabase:
        return None

    for variante in _variantes_telefone(telefone):
        try:
            resp = (
                supabase.table('clientes')
                .select('id, nome, telefone, telegram_chat_id')
                .eq('telefone', variante)
                .execute()
            )
            if resp.data:
                return resp.data[0]
        except Exception as e:
            logger.error(f"Erro ao buscar telefone '{variante}': {e}")

    return None


def _salvar_chat_id(cliente_id: str, chat_id: int) -> bool:
    """
    Atualiza clientes.telegram_chat_id com o chat_id do Telegram.
    Retorna True em caso de sucesso.
    """
    try:
        supabase.table('clientes').update(
            {'telegram_chat_id': str(chat_id)}
        ).eq('id', cliente_id).execute()
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar chat_id para cliente {cliente_id}: {e}")
        return False


def _buscar_tipos_pendentes_do_cliente(cliente_id: str) -> set[str]:
    """
    Retorna o conjunto de nomes de tipo_pesquisa que o cliente tem
    em compras ainda sem pesquisa enviada.

    Exemplo de retorno: {'VENDA', 'POS_VENDA'}
    """
    try:
        resp_compras = (
            supabase.table('compras')
            .select('id, tipos_pesquisa(nome)')
            .eq('cliente_id', cliente_id)
            .execute()
        )
        if not resp_compras.data:
            return set()

        compra_ids = [c['id'] for c in resp_compras.data]

        resp_pesquisas = (
            supabase.table('pesquisas')
            .select('compra_id')
            .in_('compra_id', compra_ids)
            .execute()
        )
        ids_com_pesquisa = {r['compra_id'] for r in resp_pesquisas.data}

        tipos = set()
        for compra in resp_compras.data:
            if compra['id'] not in ids_com_pesquisa:
                nome_tipo = (compra.get('tipos_pesquisa') or {}).get('nome')
                if nome_tipo:
                    tipos.add(nome_tipo)

        return tipos

    except Exception as e:
        logger.error(f"Erro ao verificar pendentes do cliente {cliente_id}: {e}")
        return set()


# =============================================================================
# BLOCO 3 — DISPARO IMEDIATO POS-REGISTRO
# =============================================================================

def _disparar_em_thread(tipo_pesquisa_nome: str):
    """
    Executa disparar_pesquisas() em thread separada via ThreadPoolExecutor.

    Por que nao asyncio.create_task()?
    disparar_pesquisas() e sincrono e usa asyncio.run() internamente.
    Chamar asyncio.run() dentro de um loop ativo gera RuntimeError.
    run_in_executor isola a execucao em uma thread sem tocar no loop do bot.
    """
    if not _SENDER_DISPONIVEL:
        logger.warning("[disparo imediato] message_sender.py nao encontrado.")
        return
    try:
        logger.info(f"[disparo imediato] Iniciando tipo: {tipo_pesquisa_nome}")
        _disparar_pesquisas(tipo_pesquisa_nome=tipo_pesquisa_nome)
    except Exception as e:
        logger.error(f"[disparo imediato] Erro ao disparar '{tipo_pesquisa_nome}': {e}")


# =============================================================================
# BLOCO 4 — TECLADO NATIVO DE CONTATO
# =============================================================================

def _teclado_compartilhar_contato() -> ReplyKeyboardMarkup:
    """
    Cria teclado com botao nativo request_contact.

    IMPORTANTE: este botao so aparece no app mobile do Telegram (iOS/Android).
    No Telegram Web e Desktop o botao NAO e renderizado — o bot trata isso
    via handle_telefone_digitado(), que aceita o numero como texto.
    """
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton("📱 Compartilhar meu contato", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Ou digite seu telefone aqui...",
    )


# =============================================================================
# BLOCO 5 — LÓGICA CENTRAL DE REGISTRO (compartilhada entre mobile e desktop)
# =============================================================================

async def _registrar_cliente(
    update: Update,
    telefone: str,
    origem: str,  # 'contato_nativo' | 'texto_digitado'
) -> None:
    """
    Núcleo do fluxo de registro: busca o cliente, salva o chat_id e dispara
    pesquisas pendentes. Chamada tanto pelo handler de contato nativo (mobile)
    quanto pelo handler de texto (desktop/web).

    Args:
        update   : objeto Update do python-telegram-bot
        telefone : numero bruto vindo do contato nativo ou do texto do usuario
        origem   : string de log para rastrear de onde veio o numero
    """
    chat_id = update.effective_user.id
    logger.info(f"[{origem}] telefone={telefone} | chat_id={chat_id}")

    if not supabase:
        await update.message.reply_text(
            "Servico temporariamente indisponivel. Tente novamente em instantes.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # Passo 1: buscar cliente pelo telefone
    cliente = _buscar_cliente_por_telefone(telefone)

    if not cliente:
        logger.warning(f"[{origem}] Telefone nao encontrado na base: {telefone}")
        await update.message.reply_text(
            text=(
                "Nao encontramos seu cadastro com este numero. 😕\n\n"
                "Verifique se digitou corretamente e tente novamente, "
                "ou entre em contato com a concessionaria para regularizar seu registro."
            ),
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    nome_cliente = (cliente.get('nome') or '').title() or "Cliente"

    # Passo 2: verificar se chat_id ja esta registrado (idempotencia)
    if cliente.get('telegram_chat_id') == str(chat_id):
        await update.message.reply_text(
            text=(
                f"Ola, <b>{nome_cliente}</b>! ✅\n\n"
                f"Seu contato ja estava registrado.\n"
                f"Caso tenha pesquisas pendentes, voce as recebera em breve."
            ),
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove(),
        )
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            _executor,
            lambda: reenviar_pesquisas_pendentes(cliente['id'], str(chat_id))
        )
        return

    # Passo 3: salvar o chat_id
    salvo = _salvar_chat_id(cliente['id'], chat_id)

    if not salvo:
        await update.message.reply_text(
            "Ocorreu um erro ao salvar seu cadastro. Por favor, tente novamente.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    logger.info(
        f"[{origem}] Registrado — chat_id {chat_id} → "
        f"cliente {cliente['id']} ({cliente.get('nome', '')})"
    )

    # Passo 4: confirmar para o cliente
    await update.message.reply_text(
        text=(
            f"Cadastro confirmado, <b>{nome_cliente}</b>! ✅\n\n"
            f"Voce recebera nossa pesquisa de satisfacao aqui no Telegram "
            f"em instantes. 📋"
        ),
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )

    # Passo 5: disparar pesquisas pendentes deste cliente em thread separada
    tipos_pendentes = _buscar_tipos_pendentes_do_cliente(cliente['id'])

    if tipos_pendentes:
        logger.info(
            f"[{origem}] Disparando tipos pendentes para cliente "
            f"{cliente['id']}: {tipos_pendentes}"
        )
        loop = asyncio.get_running_loop()
        for tipo in tipos_pendentes:
            loop.run_in_executor(_executor, _disparar_em_thread, tipo)
    else:
        logger.info(f"[{origem}] Nenhuma compra pendente para cliente {cliente['id']}")


# =============================================================================
# BLOCO 6 — HANDLERS
# =============================================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler para /start — ponto de entrada do cliente no bot.

    Exibe o botao nativo (visivel apenas no app mobile) E instrui o usuario
    a digitar o numero caso esteja no Telegram Web ou Desktop.
    """
    nome_telegram = update.effective_user.first_name or "Cliente"

    await update.message.reply_text(
        text=(
            f"Ola, <b>{nome_telegram}</b>! 👋\n\n"
            f"Para receber nossa pesquisa de satisfacao, precisamos confirmar seu cadastro.\n\n"
            f"<b>No celular:</b> clique no botao abaixo para compartilhar seu contato.\n\n"
            f"<b>No computador:</b> digite seu numero de telefone com DDD "
            f"(ex: <code>11999990001</code>) e pressione Enter."
        ),
        parse_mode="HTML",
        reply_markup=_teclado_compartilhar_contato(),
    )
    logger.info(
        f"[/start] user_id={update.effective_user.id} | "
        f"username=@{update.effective_user.username}"
    )


async def handle_contato(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler para contato nativo do Telegram (botao mobile request_contact).
    O numero vem verificado pelo proprio Telegram — nao precisa de validacao extra.
    """
    telefone = update.message.contact.phone_number
    await _registrar_cliente(update, telefone, origem="contato_nativo")


async def handle_telefone_digitado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler para usuarios que digitam o numero manualmente (Telegram Web/Desktop).

    Acionado apenas quando o texto parece um numero de telefone valido
    (minimo 10 digitos). Textos que nao se encaixam nesse padrao caem
    em handle_texto_inesperado.
    """
    texto = update.message.text.strip()
    await _registrar_cliente(update, texto, origem="texto_digitado")


async def handle_texto_inesperado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Orienta o usuario que enviou texto que nao parece um numero de telefone.
    """
    await update.message.reply_text(
        text=(
            "Nao entendi sua mensagem. 🤔\n\n"
            "Envie /start e siga as instrucoes para vincular seu contato.\n\n"
            "Se estiver no computador, digite seu numero com DDD "
            "(ex: <code>11999990001</code>) depois de enviar /start."
        ),
        parse_mode="HTML",
    )


async def handle_erro(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handler centralizado de erros — loga sem derrubar o bot."""
    logger.error(
        f"Erro no update '{update}': {context.error}",
        exc_info=context.error,
    )


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

def main():
    """
    Inicializa e roda o bot em modo polling.

    Ordem dos handlers importa:
      1. /start
      2. Contato nativo (mobile) — filtro filters.CONTACT
      3. Texto que parece telefone (desktop/web) — filtro customizado
      4. Qualquer outro texto — fallback de orientacao
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN nao configurado. Encerrando.")
        return

    if not supabase:
        logger.critical("Supabase nao inicializado. Encerrando.")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Filtro customizado: texto com >= 10 digitos apos limpeza = telefone
    filtro_telefone = filters.TEXT & ~filters.COMMAND & filters.Regex(r'[\d\s\(\)\+\-\.]{10,}')

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contato))
    app.add_handler(MessageHandler(filtro_telefone, handle_telefone_digitado))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_texto_inesperado))
    app.add_error_handler(handle_erro)

    logger.info("Bot receptor iniciado. Aguardando mensagens... (Ctrl+C para parar)")
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()