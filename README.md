# 📋 CRM Pesquisas de Satisfação

Sistema de automação para envio e coleta de pesquisas de satisfação de clientes de concessionárias, integrado ao **Telegram**, **Supabase** e **Google Forms**.

---

## 🧩 Visão Geral

O sistema lê arquivos CSV com dados de clientes e veículos, popula um banco de dados no Supabase e envia automaticamente o link da pesquisa para cada cliente via Telegram. Quando o cliente responde o formulário no Google Forms, as respostas são gravadas no banco e uma mensagem de encerramento é enviada imediatamente.

```
CSV → file_ingestion.py → Supabase
                               ↓
               bot_receiver.py ← cliente interage no Telegram
                               ↓
             message_sender.py → link da pesquisa via Telegram
                               ↓
              cliente responde o Google Forms
                               ↓
              Apps Script → respostas no Supabase + mensagem de encerramento
```

---

## 🗂️ Estrutura do Projeto

```
crm-pesquisas/
├── scripts/
│   ├── file_ingestion.py      # Monitora /inbox e popula o banco
│   ├── bot_receiver.py        # Bot Telegram — captura o chat_id do cliente
│   ├── message_sender.py      # Envia o link da pesquisa via Telegram
│   └── .env                   # Variáveis de ambiente (não versionar)
├── inbox/                     # Pasta monitorada para CSVs (um nível acima de scripts/)
├── .gitignore
└── README.md
```

---

## ⚙️ Pré-requisitos

- Python 3.10+
- Conta no [Supabase](https://supabase.com)
- Bot no Telegram criado via [@BotFather](https://t.me/BotFather)
- Formulário no [Google Forms](https://forms.google.com) com Apps Script configurado

### Instalação das dependências

```bash
pip install supabase python-dotenv watchdog pandas "python-telegram-bot[job-queue]"
```

---

## 🔑 Configuração do `.env`

Crie um arquivo `.env` dentro da pasta `scripts/`:

```env
SUPABASE_URL=https://SEU_PROJETO.supabase.co
SUPABASE_KEY=SUA_SERVICE_ROLE_KEY

TELEGRAM_BOT_TOKEN=SEU_BOT_TOKEN

GOOGLE_FORMS_VENDA_URL=https://docs.google.com/forms/d/e/FORM_ID_VENDA/viewform?usp=pp_url
GOOGLE_FORMS_VENDA_ENTRY=1058293847

GOOGLE_FORMS_POS_VENDA_URL=https://docs.google.com/forms/d/e/FORM_ID_POS/viewform?usp=pp_url
GOOGLE_FORMS_POS_VENDA_ENTRY=9876543210
```

> **`SUPABASE_KEY`**: use a chave `service_role` (não a `anon`) para garantir escrita sem restrições de RLS.  
> **`GOOGLE_FORMS_*_ENTRY`**: o número após `entry.` — obtido em **Pré-preencher link** dentro do Forms.

---

## 🚀 Como usar

### 1. Iniciar o monitoramento de CSVs

```bash
cd scripts/
python file_ingestion.py
```

O script processa todos os CSVs já presentes em `inbox/` e entra em modo de monitoramento contínuo.

### 2. Iniciar o bot do Telegram (outro terminal)

```bash
cd scripts/
python bot_receiver.py
```

Os dois processos precisam rodar **em paralelo** enquanto o sistema estiver operando.

### 3. Depositar o CSV na pasta `inbox/`

O nome do arquivo define a marca e o tipo de pesquisa:

| Prefixo | Tipo de pesquisa | Exemplo |
|---------|-----------------|---------|
| `VD_`   | `VENDA`         | `VD_CHERYAMERICANA.csv` |
| `PV_`   | `POS_VENDA`     | `PV_HYUNDAICENTRO.csv`  |

**Colunas do CSV** (separador `;`):

| Coluna | Obrigatória | Observação |
|--------|------------|------------|
| `TELEFONE` | ✅ | Qualquer formato — caracteres não numéricos são removidos |
| `PLACA`    | ✅ | Chave de deduplicação do veículo |
| `NOME`     | —  | Convertido para maiúsculas |
| `CIDADE`   | —  | Convertido para maiúsculas; padrão: `DESCONHECIDA` |
| `MODELO`   | —  | Modelo do veículo |

### 4. Disparo manual de pesquisas

```bash
# Disparar pesquisas de venda
python message_sender.py --tipo VENDA

# Disparar pesquisas de pós-venda
python message_sender.py --tipo POS_VENDA

# Simular sem enviar nada (dry-run)
python message_sender.py --tipo VENDA --dry-run
```

---

## 🗄️ Schema do Banco (Supabase)

```
clientes        → id, nome, telefone, cidade, telegram_chat_id
marcas          → id, nome
veiculos        → id, placa, modelo, marca_id
tipos_pesquisa  → id, nome, descricao
compras         → id, cliente_id, veiculo_id, tipo_pesquisa_id, hash_compra, data_compra
perguntas       → id, tipo_pesquisa_id, pergunta, ordem, ativa
pesquisas       → id, compra_id, tipo_pesquisa_id, token, respondida, enviada, data_envio, data_resposta
respostas       → id, pesquisa_id, pergunta_id, resposta
```

**Deduplicação de compras:** o campo `hash_compra` é um SHA-256 de `cliente_id + veiculo_id + tipo_pesquisa_id`. O mesmo CSV pode ser depositado múltiplas vezes sem criar registros duplicados.

---

## 📱 Fluxo do cliente

1. Empresa envia `t.me/NomeDoBot` via WhatsApp ou e-mail
2. Cliente abre o Telegram e envia `/start`
3. **Mobile:** clica em "Compartilhar meu contato" → número verificado pelo Telegram  
   **Desktop/Web:** digita o número com DDD no chat
4. Bot localiza o cliente no banco pelo telefone e salva o `telegram_chat_id`
5. Cliente recebe o link da pesquisa no Telegram (Google Forms com token pré-preenchido)
6. Cliente responde o formulário
7. Apps Script salva as respostas no Supabase e envia mensagem de encerramento no Telegram

---

## 🔗 Integração Google Forms + Apps Script

O Apps Script conecta o Google Forms ao Supabase. Configure-o em **Extensões > Apps Script** dentro do formulário:

```javascript
const SUPABASE_URL       = 'https://SEU_PROJETO.supabase.co';
const SUPABASE_KEY       = 'SUA_SERVICE_ROLE_KEY';
const TELEGRAM_BOT_TOKEN = 'SEU_BOT_TOKEN';
const CONTATO_EMAIL      = 'seuemail@gmail.com';
```

Após colar o código, configure o trigger:  
**Relógios → Adicionar gatilho → `onFormSubmit` → Ao enviar formulário**

Antes de ativar, teste a conexão selecionando `testarConexao` no dropdown e clicando em ▶.

---

## 🛠️ Tecnologias

| Tecnologia | Uso |
|------------|-----|
| Python 3.10+ | Scripts de automação |
| [Supabase](https://supabase.com) | Banco de dados PostgreSQL + REST API |
| [python-telegram-bot](https://python-telegram-bot.org) | Envio e recebimento via Telegram |
| [watchdog](https://pypi.org/project/watchdog/) | Monitoramento de pasta em tempo real |
| [pandas](https://pandas.pydata.org) | Leitura e limpeza dos CSVs |
| Google Forms + Apps Script | Coleta de respostas |

---

## 📄 Licença

Projeto de uso interno. Todos os direitos reservados.
