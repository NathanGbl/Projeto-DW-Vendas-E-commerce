set -e
set -o pipefail

PROJECT_NAME="Projeto-DW-Vendas-E-commerce"
SPARK_VERSION="4.0.1"
HADOOP_PACKAGES=(
    pyspark==4.0.1
    pandas
    pyarrow
    fastapi
    uvicorn[standard]
    httpx
    minio
    python-dotenv
)

echo "Instalando pacotes do sistema..."

sudo apt update -y
sudo apt install -y curl wget unzip build-essential python3 python3-venv python3-pip openjdk-17-jdk

if ! command -v uv &> /dev/null; then
    echo "Instalando uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
else
    echo "uv já está instalado"

fi
echo "Versão do uv: "
uv --version

SPARK_DIR="$HOME/spark"
if [ ! -d "$SPARK_DIR" ]; then
    echo "Baixando Spark $SPARK_VERSION com Hadoop $HADOOP_VERSION..."
    wget -q "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
    tar -xzf /tmp/spark.tgz -C "$HOME"
    mv "$HOME/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "$SPARK_DIR"
else
    echo "Spark já configurado em $SPARK_DIR"
fi

echo "Configurando variáveis de ambiente..."
{
  echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
  echo "export SPARK_HOME=$SPARK_DIR"
  echo "export PATH=\$SPARK_HOME/bin:\$PATH"
} >> "$HOME/.bashrc"

source "$HOME/.bashrc"

PROJECT_PATH="$HOME/Documents/$PROJECT_NAME"
mkdir -p "$PROJECT_PATH"
cd "$PROJECT_PATH"

echo "Instalando dependências Python..."
for pkg in "${PYTHON_PACKAGES[@]}"; do
  echo "→ $pkg"
  uv add "$pkg"
done
