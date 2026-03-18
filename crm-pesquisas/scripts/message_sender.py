"""
message_sender.py — Disparo de pesquisas de satisfação via Telegram

Usa a biblioteca oficial python-telegram-bot para envio.
O Bot aqui funciona apenas como REMETENTE (envia mensagens outbound).
A captura do chat_id dos clientes é responsabilidade do bot_receiver.py.

Instalação:
    pip install "python-telegram-bot[job-queue]" supabase python-dotenv

Uso direto (CLI):
    python message_sender.py --tipo VENDA
    python message_sender.py --tipo POS_VENDA
    python message_sender.py --tipo VENDA --dry-run

Uso integrado (chamado pelo file_ingestion.py ou bot_receiver.py):
    from message_sender import disparar_pesquisas
    disparar_pesquisas("VENDA")
"""

import os
import time
import uuid
import asyncio
import argparse
import logging
from datetime import datetime

from telegram import Bot
from telegram.error import (
    Forbidden,       # 403 — bot bloqueado pelo usuário
    BadRequest,      # 400 — chat_id inválido / usuário nunca iniciou o bot
    RetryAfter,      # 429 — rate limit; traz o tempo de espera embutido
    TelegramError,   # base para qualquer outro erro da API
)
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

SUPABASE_URL       = os.getenv("SUPABASE_URL")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

GOOGLE_FORMS_URLS = {
    "VENDA":     os.getenv("GOOGLE_FORMS_VENDA_URL", ""),
    "POS_VENDA": os.getenv("GOOGLE_FORMS_POS_VENDA_URL", ""),
}
GOOGLE_FORMS_ENTRIES = {
    "VENDA":     os.getenv("GOOGLE_FORMS_VENDA_ENTRY", ""),
    "POS_VENDA": os.getenv("GOOGLE_FORMS_POS_VENDA_ENTRY", ""),
}
# --- Logging ---
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# --- Cliente Supabase ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    logger.error(f"Erro ao inicializar Supabase: {e}")
    supabase = None

def montar_link(token: str, tipo_pesquisa: str) -> str:
    """Gera a URL do Google Forms com o token pré-preenchido via parâmetro de URL."""
    base  = GOOGLE_FORMS_URLS.get(tipo_pesquisa.upper(), "")
    entry = GOOGLE_FORMS_ENTRIES.get(tipo_pesquisa.upper(), "")
    if not base or not entry:
        raise ValueError(f"URL ou entry ID não configurados para tipo '{tipo_pesquisa}'. Verifique o .env.")
    return f"{base}&entry.{entry}={token}"

# =============================================================================
# BLOCO 1 — CONSULTAS AO BANCO
# =============================================================================

def obter_tipo_pesquisa(nome: str) -> dict | None:
    """Retorna {id, nome} do tipo de pesquisa ou None se não encontrado."""
    try:
        resp = (
            supabase.table('tipos_pesquisa')
            .select('id, nome')
            .eq('nome', nome.upper())
            .execute()
        )
        return resp.data[0] if resp.data else None
    except Exception as e:
        logger.error(f"Erro ao buscar tipo de pesquisa '{nome}': {e}")
        return None


def buscar_compras_pendentes(tipo_pesquisa_id: str, cliente_id: str = None) -> list[dict]:
    """
    Retorna as compras que ainda NÃO tem pesquisa registrada.
    Se cliente_id for informado, filtra apenas para aquele cliente (disparo imediato).
    """
    try:
        # 1. Busca compras do tipo informado
        query = supabase.table('compras').select(
            'id, cliente_id, data_compra, hash_compra, '
            'clientes(id, nome, telegram_chat_id), '
            'veiculos(id, modelo, placa, marcas(nome))'
        ).eq('tipo_pesquisa_id', tipo_pesquisa_id)
        
        if cliente_id:
            query = query.eq('cliente_id', cliente_id)
            
        resp_compras = query.execute()
        
        if not resp_compras.data:
            return []

        # 2. Filtra compras que já possuem registro na tabela 'pesquisas'
        compra_ids = [c['id'] for c in resp_compras.data]
        resp_enviadas = (
            supabase.table('pesquisas')
            .select('compra_id')
            .in_('compra_id', compra_ids)
            .execute()
        )
        ids_enviados = {r['compra_id'] for r in resp_enviadas.data}

        return [c for c in resp_compras.data if c['id'] not in ids_enviados]

    except Exception as e:
        logger.error(f"Erro ao buscar compras pendentes: {e}")
        return []


def registrar_pesquisa(compra_id: str, tipo_pesquisa_id: str) -> str | None:
    """
    Insere linha em 'pesquisas' e retorna o token UUID gerado.
    """
    token = str(uuid.uuid4())
    try:
        supabase.table('pesquisas').insert({
            'tipo_pesquisa_id': tipo_pesquisa_id,
            'compra_id':        compra_id,
            'token':            token,
            'respondida':       False,
            'data_envio':       datetime.now().isoformat(),
        }).execute()
        return token
    except Exception as e:
        logger.error(f"Erro ao registrar pesquisa (compra {compra_id}): {e}")
        return None


def marcar_envio_falhou(token: str):
    """Remove o registro da pesquisa se o envio falhar para permitir re-tentativa."""
    try:
        supabase.table('pesquisas').delete().eq('token', token).execute()
    except Exception as e:
        logger.error(f"Erro ao remover pesquisa falha: {e}")


def buscar_pesquisas_nao_respondidas(cliente_id: str) -> list[dict]:
    """
    Retorna pesquisas já registradas na tabela 'pesquisas' que ainda não foram
    respondidas (respondida=False) para um cliente específico.

    Usada no reenvio: quando o cliente dá /start novamente e já tem chat_id,
    o bot_receiver chama reenviar_pesquisas_pendentes() para reenviar os links
    cujo prazo de resposta ainda não foi cumprido.

    Retorna lista de dicts com tudo necessário para remontar a mensagem:
    token, tipo_pesquisa_nome, e dados de compra/cliente/veículo.
    """
    try:
        resp = (
            supabase.table('pesquisas')
            .select(
                'id, token, respondida, enviada, tipo_pesquisa_id, compra_id, '
                'compras(id, data_compra, cliente_id, '
                '  clientes(id, nome, telegram_chat_id), '
                '  veiculos(id, modelo, placa, marcas(nome))), '
                'tipos_pesquisa(nome)'
            )
            .eq('respondida', False)
            .execute()
        )

        if not resp.data:
            return []

        # Filtra pelo cliente_id navegando pelo join aninhado
        resultado = []
        for p in resp.data:
            compra  = p.get('compras') or {}
            cliente = compra.get('clientes') or {}
            if cliente.get('id') == cliente_id:
                resultado.append(p)

        return resultado

    except Exception as e:
        logger.error(f"Erro ao buscar pesquisas não respondidas do cliente {cliente_id}: {e}")
        return []


def reenviar_pesquisas_pendentes(cliente_id: str, chat_id: str) -> dict:
    """
    Reenvio de pesquisas não respondidas para um cliente que já tem chat_id.

    Chamada pelo bot_receiver.py quando o cliente envia /start e o chat_id
    já está cadastrado — garante que pesquisas perdidas ou ignoradas sejam
    reenviadas com o mesmo token (não gera duplicata no banco).

    Fluxo por pesquisa encontrada:
      1. Reutiliza o token existente (não cria nova linha em 'pesquisas')
      2. Remonta o link com o token original
      3. Reenvia a mensagem via Telegram
      4. Atualiza enviada=True e data_envio se o envio for bem-sucedido

    Args:
        cliente_id : UUID do cliente em clientes.id
        chat_id    : telegram_chat_id do cliente (já confirmado no banco)

    Returns:
        dict com contadores: reenviados, erros_telegram, erros_link
    """
    resultado = {"reenviados": 0, "erros_telegram": 0, "erros_link": 0}

    pendentes = buscar_pesquisas_nao_respondidas(cliente_id)

    if not pendentes:
        logger.info(f"[reenvio] Nenhuma pesquisa pendente para cliente {cliente_id}")
        return resultado

    logger.info(f"[reenvio] {len(pendentes)} pesquisa(s) não respondida(s) para cliente {cliente_id}")

    for pesquisa in pendentes:
        token          = pesquisa.get('token')
        tipo_nome      = (pesquisa.get('tipos_pesquisa') or {}).get('nome', '')
        compra         = pesquisa.get('compras') or {}
        cliente        = compra.get('clientes') or {}
        veiculo        = compra.get('veiculos') or {}
        marca          = (veiculo.get('marcas') or {}).get('nome', 'CONCESSIONÁRIA')
        nome           = cliente.get('nome', '')
        data_compra    = compra.get('data_compra', '')
        modelo         = veiculo.get('modelo', '')
        placa          = veiculo.get('placa', '')

        try:
            link = montar_link(token, tipo_nome)
        except ValueError as e:
            logger.error(f"[reenvio] {e}")
            resultado["erros_link"] += 1
            continue

        msg = montar_mensagem(nome, marca, data_compra, modelo, placa, link)
        ok, motivo = enviar_mensagem(chat_id, msg)

        if ok:
            logger.info(f"[reenvio] ✓ Reenviado para {nome} | tipo={tipo_nome} | token={token[:8]}...")
            try:
                supabase.table('pesquisas').update({
                    'enviada':    True,
                    'data_envio': datetime.now().isoformat(),
                }).eq('token', token).execute()
            except Exception as eu:
                logger.warning(f"[reenvio] Reenviado ao Telegram mas falhou ao marcar enviada=True: {eu}")
            resultado["reenviados"] += 1
        else:
            logger.error(f"[reenvio] Falha ao reenviar para {nome}: {motivo}")
            resultado["erros_telegram"] += 1

        time.sleep(0.05)

    return resultado


# =============================================================================
# BLOCO 2 — TELEGRAM
# =============================================================================

def montar_mensagem(nome_cliente: str, marca: str, data_compra: str, modelo: str, placa: str, link: str) -> str:
    """Monta o texto da mensagem conforme o template solicitado."""
    nome = nome_cliente.title() if nome_cliente else "Sr.(a)"
    
    # Formatação da data
    try:
        dt = datetime.fromisoformat(data_compra.replace('Z', '+00:00'))
        data_formatada = dt.strftime('%d/%m/%Y')
    except:
        data_formatada = data_compra

    mensagem = (
        f"Olá <b>{nome}</b>,\n\n"
        f"Recentemente o(a) Sr.(a) esteve em nossa concessionária do <b>GRUPO {marca}</b> em <b>{data_formatada}</b> "
        f"para uma manutenção em seu veículo <b>{modelo}</b> placa <b>{placa}</b>.\n\n"
        f"Estamos sempre em busca de melhorar e oferecer o melhor serviço possível. "
        f"Para isso, gostaríamos de contar com a sua opinião!\n\n"
        f"<b>GRUPO {marca}</b>\n"
        f"<a href='{link}'>RESPONDER PESQUISA</a>"
    )
    return mensagem


async def _enviar_async(chat_id: str, texto: str) -> tuple[bool, str]:
    """Corrotina de envio via Telegram."""
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    async with bot:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=texto,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return True, ""
        except Forbidden as e:
            return False, f"BOT_BLOQUEADO: {e.message}"
        except BadRequest as e:
            return False, f"CHAT_INVALIDO: {e.message}"
        except RetryAfter as e:
            logger.warning(f"Rate limit. Aguardando {e.retry_after}s...")
            await asyncio.sleep(e.retry_after)
            return await _enviar_async(chat_id, texto)
        except TelegramError as e:
            return False, f"TELEGRAM_ERROR: {e.message}"


def enviar_mensagem(chat_id: str, texto: str) -> tuple[bool, str]:
    """Bridge síncrono para envio de mensagem."""
    try:
        return asyncio.run(_enviar_async(chat_id, texto))
    except RuntimeError:
        # Caso já exista um loop rodando (ex: dentro do bot_receiver)
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_enviar_async(chat_id, texto))


# =============================================================================
# BLOCO 3 — ORQUESTRADOR PRINCIPAL
# =============================================================================

def disparar_pesquisas(tipo_pesquisa_nome: str, cliente_id: str = None, dry_run: bool = False) -> dict:
    """
    Função principal de disparo.
    """
    resultado = {
        "enviados":       0,
        "sem_chat_id":    0,
        "bot_bloqueado":  0,
        "erros_telegram": 0,
        "erros_db":       0,
    }

    if not supabase or not TELEGRAM_BOT_TOKEN:
        logger.error("Configurações incompletas (Supabase ou Telegram).")
        return resultado

    tipo = obter_tipo_pesquisa(tipo_pesquisa_nome)
    if not tipo:
        logger.error(f"Tipo '{tipo_pesquisa_nome}' não encontrado.")
        return resultado

    tipo_pesquisa_id = tipo['id']
    
    logger.info(f"Iniciando disparos para: {tipo_pesquisa_nome}")
    
    pendentes = buscar_compras_pendentes(tipo_pesquisa_id, cliente_id)
    if not pendentes:
        logger.info("Nenhuma compra pendente encontrada.")
        return resultado

    for i, compra in enumerate(pendentes, start=1):
        try:
            cliente = compra.get('clientes') or {}
            veiculo = compra.get('veiculos') or {}
            marca   = veiculo.get('marcas', {}).get('nome', 'CONCESSIONÁRIA')
            
            chat_id = cliente.get('telegram_chat_id')
            nome    = cliente.get('nome', '')
            
            if not chat_id:
                resultado["sem_chat_id"] += 1
                continue

            if dry_run:
                link = montar_link("DRY-RUN-TOKEN", tipo_pesquisa_nome)
                msg  = montar_mensagem(
                    nome,
                    marca,
                    datetime.now().strftime('%Y-%m-%d'),
                    veiculo.get('modelo', 'MODELO TESTE'),
                    veiculo.get('placa', 'XXX-0000'),
                    link,
                )
                logger.info(f"    [DRY-RUN] {msg[:120]}...")
                resultado["enviados"] += 1
                continue

            # 1. Registra a intenção de envio
            token = registrar_pesquisa(compra['id'], tipo_pesquisa_id)
            if not token:
                resultado["erros_db"] += 1
                continue

            # 2. Monta e envia
            link = montar_link(token, tipo_pesquisa_nome)
            msg  = montar_mensagem(
                nome, marca, compra['data_compra'], 
                veiculo.get('modelo', ''), veiculo.get('placa', ''), link
            )
            
            ok, motivo = enviar_mensagem(chat_id, msg)

            if ok:
                logger.info(f"✓ Enviado para {nome}")
                try:
                    supabase.table('pesquisas').update({
                        'enviada':    True,
                        'data_envio': datetime.now().isoformat(),
                    }).eq('token', token).execute()
                except Exception as eu:
                    logger.warning(f"Enviado ao Telegram mas falhou ao marcar enviada=True: {eu}")
                resultado["enviados"] += 1
            else:
                marcar_envio_falhou(token)
                if "BOT_BLOQUEADO" in motivo:
                    resultado["bot_bloqueado"] += 1
                else:
                    logger.error(f"Falha ao enviar para {nome}: {motivo}")
                    resultado["erros_telegram"] += 1

            # Rate limit preventivo
            time.sleep(0.05)

        except Exception as e:
            logger.error(f"Erro ao processar compra {compra.get('id')}: {e}")
            resultado["erros_db"] += 1

    return resultado


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dispara pesquisas via Telegram.")
    parser.add_argument("--tipo", required=True, choices=["VENDA", "POS_VENDA"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Valida variáveis obrigatórias antes de executar
    faltando = [
        k for k, v in {
            "SUPABASE_URL":                 SUPABASE_URL,
            "SUPABASE_KEY":                 SUPABASE_KEY,
            "TELEGRAM_BOT_TOKEN":           TELEGRAM_BOT_TOKEN,
            "GOOGLE_FORMS_VENDA_URL":       GOOGLE_FORMS_URLS["VENDA"],
            "GOOGLE_FORMS_VENDA_ENTRY":     GOOGLE_FORMS_ENTRIES["VENDA"],
            "GOOGLE_FORMS_POS_VENDA_URL":   GOOGLE_FORMS_URLS["POS_VENDA"],
            "GOOGLE_FORMS_POS_VENDA_ENTRY": GOOGLE_FORMS_ENTRIES["POS_VENDA"],
        }.items() if not v
    ]
    if faltando:
        logger.error(f"Variáveis não configuradas no .env: {', '.join(faltando)}")
        raise SystemExit(1)

    disparar_pesquisas(tipo_pesquisa_nome=args.tipo, dry_run=args.dry_run)