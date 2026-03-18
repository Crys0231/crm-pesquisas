import os
import re
import time
import hashlib
import threading
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

load_dotenv()

# Integração com o disparador de pesquisas.
# O try/except garante que file_ingestion.py funciona normalmente
# mesmo que message_sender.py não esteja no mesmo diretório.
try:
    from message_sender import disparar_pesquisas
    _SENDER_DISPONIVEL = True
except ImportError:
    _SENDER_DISPONIVEL = False
    print("[aviso] message_sender.py não encontrado — disparo automático de pesquisas desativado.")

# --- Configuração do Supabase ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# --- Inicialização do Cliente Supabase ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"Erro ao inicializar o cliente Supabase: {e}")
    supabase = None

# --- Funções de Limpeza e Extração ---

def limpar_telefone(telefone):
    """Remove caracteres não numéricos do telefone."""
    if pd.isna(telefone):
        return ""
    return re.sub(r'\D', '', str(telefone))

def extrair_info_arquivo(nome_arquivo):
    """
    Extrai marca e tipo de pesquisa do nome do arquivo com base no prefixo.
    Padrão esperado: [PREFIXO]_MARCA[CIDADE].csv
    Exemplo: PV_CHERYAMERICANA.csv -> prefixo='PV', marca='CHERY', tipo_pesquisa='POS_VENDA'
    """
    nome_base = os.path.splitext(nome_arquivo)[0]

    prefixo = ""
    nome_sem_prefixo = nome_base
    if "_" in nome_base:
        partes = nome_base.split("_", 1)
        prefixo = partes[0].upper()
        nome_sem_prefixo = partes[1]

    tipo_pesquisa_mapeado = "VENDA"  # Default
    if prefixo == "PV":
        tipo_pesquisa_mapeado = "POS_VENDA"
    elif prefixo == "VD":
        tipo_pesquisa_mapeado = "VENDA"
    # Adicione mais mapeamentos de prefixo conforme necessário aqui

    marcas_conhecidas = [
        'HYUNDAI', 'PEUGEOT', 'CHERY', 'VOLKSWAGEN',
        'CHEVROLET', 'FORD', 'OMODA', 'RENAULT',
        'MITSUBISHI', 'FIAT'
    ]

    marca_encontrada = 'DESCONHECIDA'
    for marca in marcas_conhecidas:
        if marca in nome_sem_prefixo.upper():
            marca_encontrada = marca.upper()
            break

    return marca_encontrada, tipo_pesquisa_mapeado

# --- Funções de Interação com o Banco de Dados ---

def obter_ou_criar_id(tabela, coluna, valor):
    """Função genérica para obter ou criar um registro e retornar seu ID."""
    try:
        response = supabase.table(tabela).select('id').eq(coluna, valor).execute()
        if response.data:
            return response.data[0]['id']

        response = supabase.table(tabela).insert({coluna: valor}).execute()
        return response.data[0]['id']
    except Exception as e:
        print(f"Erro ao processar '{valor}' na tabela '{tabela}': {e}")
        return None

def gerar_hash_compra(cliente_id, veiculo_id, tipo_pesquisa_id):
    """Gera um hash único para a compra para evitar duplicatas."""
    dados = f"{cliente_id}_{veiculo_id}_{tipo_pesquisa_id}"
    return hashlib.sha256(dados.encode()).hexdigest()

def processar_cliente(telefone, nome, cidade):
    """Insere ou atualiza um cliente e retorna seu ID."""
    try:
        response = supabase.table('clientes').select('id').eq('telefone', telefone).execute()
        if response.data:
            cliente_id = response.data[0]['id']
            supabase.table('clientes').update({
                'nome': nome,
                'cidade': cidade,
                'updated_at': datetime.now().isoformat()
            }).eq('id', cliente_id).execute()
            return cliente_id

        response = supabase.table('clientes').insert({
            'telefone': telefone, 'nome': nome, 'cidade': cidade
        }).execute()
        return response.data[0]['id']
    except Exception as e:
        print(f"Erro ao processar cliente com telefone '{telefone}': {e}")
        return None

def processar_veiculo(placa, modelo, marca_id):
    """Insere ou atualiza um veículo e retorna seu ID."""
    try:
        response = supabase.table('veiculos').select('id').eq('placa', placa).execute()
        if response.data:
            veiculo_id = response.data[0]['id']
            supabase.table('veiculos').update({
                'modelo': modelo, 'marca_id': marca_id
            }).eq('id', veiculo_id).execute()
            return veiculo_id

        response = supabase.table('veiculos').insert({
            'placa': placa, 'modelo': modelo, 'marca_id': marca_id
        }).execute()
        return response.data[0]['id']
    except Exception as e:
        print(f"Erro ao processar veículo com placa '{placa}': {e}")
        return None

def registrar_compra(cliente_id, veiculo_id, tipo_pesquisa_id):
    """Registra uma nova compra se ela ainda não existir."""
    try:
        hash_compra = gerar_hash_compra(cliente_id, veiculo_id, tipo_pesquisa_id)
        response = supabase.table('compras').select('id').eq('hash_compra', hash_compra).execute()

        if response.data:
            print(f"Compra já registrada para cliente {cliente_id} e veículo {veiculo_id}.")
            return response.data[0]['id']

        response = supabase.table('compras').insert({
            'cliente_id': cliente_id,
            'veiculo_id': veiculo_id,
            'tipo_pesquisa_id': tipo_pesquisa_id,
            'hash_compra': hash_compra,
            'data_compra': datetime.now().strftime('%Y-%m-%d')
        }).execute()
        return response.data[0]['id']
    except Exception as e:
        print(f"Erro ao registrar compra: {e}")
        return None

# --- Funções Principais de Processamento ---

def processar_arquivo_csv(caminho_arquivo, marca_arquivo, tipo_pesquisa_arquivo):
    """Processa as linhas de um arquivo CSV e insere os dados no Supabase."""
    try:
        df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin-1')
    except Exception as e:
        print(f"Erro fatal ao ler o arquivo {os.path.basename(caminho_arquivo)}: {e}")
        return 0, 1

    registros_processados, erros = 0, 0

    # IDs constantes por arquivo: resolvidos UMA vez antes do loop
    tipo_pesquisa_id = obter_ou_criar_id('tipos_pesquisa', 'nome', tipo_pesquisa_arquivo)
    if not tipo_pesquisa_id:
        print(f"Erro: Não foi possível obter o ID para o tipo de pesquisa '{tipo_pesquisa_arquivo}'.")
        return 0, df.shape[0]

    # FIX (Bug 4): marca_id também é constante por arquivo — resolvida aqui, fora do loop
    marca_id = obter_ou_criar_id('marcas', 'nome', marca_arquivo)
    if not marca_id:
        print(f"Erro: Não foi possível obter o ID para a marca '{marca_arquivo}'.")
        return 0, df.shape[0]

    for index, row in df.iterrows():
        try:
            telefone = limpar_telefone(row.get('TELEFONE'))
            nome = str(row.get('NOME', '')).strip().upper()
            cidade = str(row.get('CIDADE', 'DESCONHECIDA')).strip().upper()
            modelo = str(row.get('MODELO', '')).strip().upper()
            placa = str(row.get('PLACA', '')).strip().upper()

            if not telefone or not placa:
                print(f"Linha {index + 2} ignorada: Telefone ou Placa ausente.")
                erros += 1
                continue

            cliente_id = processar_cliente(telefone, nome, cidade)
            if not cliente_id:
                erros += 1
                continue

            veiculo_id = processar_veiculo(placa, modelo, marca_id)
            if not veiculo_id:
                erros += 1
                continue

            compra_id = registrar_compra(cliente_id, veiculo_id, tipo_pesquisa_id)
            if compra_id:
                registros_processados += 1
                print(f"Linha {index + 2} processada com sucesso.")
            else:
                erros += 1

        except Exception as e:
            print(f"Erro inesperado ao processar linha {index + 2}: {e}")
            erros += 1

    return registros_processados, erros

# --- Lógica de Monitoramento de Pasta ---

# Tempo de espera (em segundos) após o último evento antes de processar o arquivo.
# No Windows, editores disparam múltiplos on_modified em ondas separadas por >1s
# (uma onda para o conteúdo, outra para metadados). 2s cobre a maioria dos casos.
DEBOUNCE_DELAY = 2.0

class CSVEventHandler(FileSystemEventHandler):
    """
    Handler com debounce + rastreamento de mtime para evitar duplo processamento.

    Problema no Windows: editores disparam on_created + múltiplos on_modified
    em ondas que podem estar separadas por mais de 1 segundo, fazendo o debounce
    simples falhar (o timer dispara entre as ondas e o arquivo é processado 2x).

    Solução em duas camadas:
      1. Debounce (DEBOUNCE_DELAY): reagenda o timer a cada evento, processando
         só após N segundos de silêncio.
      2. Rastreamento de mtime: antes de processar, compara o mtime atual do
         arquivo com o mtime do último processamento. Se for igual, o arquivo
         não mudou e o evento é ignorado — proteção final contra duplicatas.
    """

    def __init__(self, diretorio_entrada):
        super().__init__()
        self.diretorio_entrada = diretorio_entrada
        self._timers: dict[str, threading.Timer] = {}
        self._ultimo_mtime: dict[str, float] = {}  # file_path -> mtime do último processamento
        self._lock = threading.Lock()

    def _agendar_processamento(self, file_path: str):
        """Cancela timer existente e agenda novo processamento com debounce."""
        with self._lock:
            timer_existente = self._timers.get(file_path)
            if timer_existente:
                timer_existente.cancel()

            novo_timer = threading.Timer(
                DEBOUNCE_DELAY,
                self._executar_processamento,
                args=[file_path]
            )
            self._timers[file_path] = novo_timer
            novo_timer.start()

    def _executar_processamento(self, file_path: str):
        """Processa o arquivo apenas se ele foi realmente modificado desde a última execução."""
        with self._lock:
            self._timers.pop(file_path, None)

        if not os.path.exists(file_path):
            print(f"Arquivo não encontrado no momento do processamento: {file_path}")
            return

        # Guarda de mtime: ignora se o arquivo não mudou desde o último processamento
        mtime_atual = os.path.getmtime(file_path)
        with self._lock:
            mtime_anterior = self._ultimo_mtime.get(file_path)
            if mtime_anterior is not None and mtime_atual == mtime_anterior:
                print(f"[IGNORADO] {os.path.basename(file_path)} não foi modificado (mtime idêntico).")
                return
            self._ultimo_mtime[file_path] = mtime_atual

        nome_arquivo = os.path.basename(file_path)
        print(f"\n[EVENTO] Processando: {nome_arquivo}")

        marca, tipo_pesquisa = extrair_info_arquivo(nome_arquivo)
        print(f"  Marca extraída:   {marca}")
        print(f"  Tipo de pesquisa: {tipo_pesquisa}")

        registros, erros = processar_arquivo_csv(file_path, marca, tipo_pesquisa)
        print(f"  -> {registros} registros processados, {erros} erros.")

        # Dispara pesquisas para as compras recém-cadastradas.
        # Só executa se houve registros novos E o sender está disponível.
        if registros > 0 and _SENDER_DISPONIVEL:
            print(f"\n  [SENDER] Iniciando disparo para tipo: {tipo_pesquisa}")
            disparar_pesquisas(tipo_pesquisa_nome=tipo_pesquisa)
        elif registros > 0 and not _SENDER_DISPONIVEL:
            print("  [SENDER] Ignorado: message_sender.py não encontrado.")

    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.csv'):
            self._agendar_processamento(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.csv'):
            self._agendar_processamento(event.src_path)


def processar_etl_inicial(diretorio_entrada):
    """Processa todos os arquivos CSV existentes no diretório na inicialização."""
    if not supabase:
        return

    if not os.path.exists(diretorio_entrada):
        print(f"Diretório de entrada '{diretorio_entrada}' não encontrado.")
        return

    total_registros, total_erros = 0, 0
    print(f"Iniciando processamento inicial de arquivos em '{diretorio_entrada}'...")

    for arquivo in os.listdir(diretorio_entrada):
        if arquivo.lower().endswith('.csv'):
            caminho_completo = os.path.join(diretorio_entrada, arquivo)
            marca, tipo_pesquisa = extrair_info_arquivo(arquivo)

            print(f"\nProcessando arquivo (inicial): {arquivo}")
            print(f"  Marca extraída:   {marca}")
            print(f"  Tipo de pesquisa: {tipo_pesquisa}")

            registros, erros = processar_arquivo_csv(caminho_completo, marca, tipo_pesquisa)
            total_registros += registros
            total_erros += erros
            print(f"  -> {registros} registros processados, {erros} erros.")

            # Dispara pesquisas para compras novas cadastradas neste arquivo
            if registros > 0 and _SENDER_DISPONIVEL:
                print(f"  [SENDER] Disparando pesquisas para tipo: {tipo_pesquisa}")
                disparar_pesquisas(tipo_pesquisa_nome=tipo_pesquisa)

    print(f"\n{'='*60}")
    print(f"RESUMO DO PROCESSAMENTO INICIAL")
    print(f"{'='*60}")
    print(f"Total inseridos/atualizados: {total_registros}")
    print(f"Total de erros:              {total_erros}")
    print(f"{'='*60}\n")


# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erro Crítico: Variáveis SUPABASE_URL e SUPABASE_KEY não configuradas.")
        print("Configure-as no sistema ou em um arquivo .env antes de executar.")
    else:
        caminho_da_pasta_inbox = '../inbox'

        processar_etl_inicial(caminho_da_pasta_inbox)

        event_handler = CSVEventHandler(caminho_da_pasta_inbox)
        observer = Observer()
        observer.schedule(event_handler, caminho_da_pasta_inbox, recursive=False)
        observer.start()

        print(f"Monitorando '{caminho_da_pasta_inbox}' para novos arquivos CSV. Ctrl+C para parar.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            print("\nAguardando encerramento do observer...")
        observer.join()
        print("Monitoramento encerrado.")