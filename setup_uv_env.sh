#!/usr/bin/env bash
# filepath: /home/nathan_gabriel/Documents/Projeto-DW-Vendas-E-commerce/setup_uv_env.sh

set -e
set -o pipefail

# ============================================
# Configurações
# ============================================
PROJECT_NAME="Projeto-DW-Vendas-E-commerce"
SPARK_VERSION="4.0.1"
HADOOP_VERSION="3"  # Spark 4.0.1 usa Hadoop 3.x
PYTHON_VERSION="3.13"

PYTHON_PACKAGES=(
    "pyspark==4.0.1"
    "pandas"
    "pyarrow"
    "fastapi"
    "uvicorn[standard]"
    "httpx"
    "minio"
    "python-dotenv"
    "sqlalchemy"
    "pyodbc"
)

# ============================================
# Instalação de pacotes do sistema
# ============================================
echo "=========================================="
echo "Instalando pacotes do sistema..."
echo "=========================================="

sudo apt update -y
sudo apt install -y \
    curl \
    wget \
    unzip \
    build-essential \
    python3 \
    python3-venv \
    python3-pip \
    openjdk-17-jdk \
    unixodbc-dev

# Instalar ODBC Driver para SQL Server (opcional, comente se não usar)
if ! odbcinst -q -d -n "ODBC Driver 18 for SQL Server" &> /dev/null; then
    echo "Instalando ODBC Driver para SQL Server..."
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
    sudo apt update -y
    sudo ACCEPT_EULA=Y apt install -y msodbcsql18 mssql-tools18
fi

# ============================================
# Detectar JAVA_HOME dinamicamente
# ============================================
if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    echo "JAVA_HOME detectado: $JAVA_HOME"
fi

# ============================================
# Instalação do uv
# ============================================
echo "=========================================="
echo "Verificando/Instalando uv..."
echo "=========================================="

if ! command -v uv &> /dev/null; then
    echo "Instalando uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
else
    echo "✓ uv já está instalado"
fi

uv --version

# ============================================
# Download e configuração do Spark
# ============================================
SPARK_DIR="$HOME/spark"

if [ ! -d "$SPARK_DIR" ]; then
    echo "=========================================="
    echo "Baixando Apache Spark ${SPARK_VERSION}..."
    echo "=========================================="
    
    SPARK_FILENAME="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
    SPARK_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_FILENAME}.tgz"
    
    echo "URL: $SPARK_URL"
    wget -q "$SPARK_URL" -O /tmp/spark.tgz
    
    echo "Extraindo Spark..."
    tar -xzf /tmp/spark.tgz -C "$HOME"
    mv "$HOME/${SPARK_FILENAME}" "$SPARK_DIR"
    rm -f /tmp/spark.tgz
    
    echo "✓ Spark instalado em $SPARK_DIR"
else
    echo "✓ Spark já configurado em $SPARK_DIR"
fi

# ============================================
# Configurar variáveis de ambiente
# ============================================
echo "=========================================="
echo "Configurando variáveis de ambiente..."
echo "=========================================="

# Remove configurações antigas se existirem
sed -i '/# >>> Spark e Java config >>>/,/# <<< Spark e Java config <<</d' "$HOME/.bashrc"

# Adiciona configurações novas
{
  echo ""
  echo "# >>> Spark e Java config >>>"
  echo "export JAVA_HOME=$JAVA_HOME"
  echo "export SPARK_HOME=$SPARK_DIR"
  echo "export PATH=\$SPARK_HOME/bin:\$JAVA_HOME/bin:\$PATH"
  echo "export PYSPARK_PYTHON=python3"
  echo "# <<< Spark e Java config <<<"
  echo ""
} >> "$HOME/.bashrc"

echo "✓ Variáveis adicionadas ao .bashrc"

# Exporta para a sessão atual
export JAVA_HOME="$JAVA_HOME"
export SPARK_HOME="$SPARK_DIR"
export PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON=python3

# ============================================
# Configurar projeto Python
# ============================================
echo "=========================================="
echo "Configurando projeto Python..."
echo "=========================================="

PROJECT_PATH="$HOME/Documents/$PROJECT_NAME"

# Se o diretório não existir, criar
if [ ! -d "$PROJECT_PATH" ]; then
    echo "Criando diretório do projeto..."
    mkdir -p "$PROJECT_PATH"
fi

cd "$PROJECT_PATH"

# Inicializar projeto uv se pyproject.toml não existir
if [ ! -f "pyproject.toml" ]; then
    echo "Inicializando projeto uv..."
    uv init --name "projeto-dw-vendas-e-commerce" --python "$PYTHON_VERSION"
fi

# ============================================
# Instalar dependências Python
# ============================================
echo "=========================================="
echo "Instalando dependências Python com uv..."
echo "=========================================="

# Sincronizar ambiente primeiro (cria .venv se não existir)
uv sync

# Adicionar pacotes
for pkg in "${PYTHON_PACKAGES[@]}"; do
    echo "→ Instalando $pkg"
    uv add "$pkg"
done

echo ""
echo "=========================================="
echo "✓ Instalação concluída!"
echo "=========================================="
echo ""
echo "Próximos passos:"
echo "  1. Execute: source ~/.bashrc"
echo "  2. Entre no projeto: cd $PROJECT_PATH"
echo "  3. Ative o ambiente: source .venv/bin/activate"
echo "  4. Configure o .env com suas credenciais"
echo "  5. Execute: uv run python src/extract/extrai_dados_api.py"
echo ""
echo "Verificações:"
echo "  - Java: $JAVA_HOME"
echo "  - Spark: $SPARK_DIR (versão $SPARK_VERSION)"
echo "  - Python: $(python3 --version)"
echo "  - uv: $(uv --version)"
echo ""