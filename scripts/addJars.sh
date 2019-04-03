JAR_DIR="../jars/"

JAR_PATH=""

cd $JAR_DIR

for entry in "$JAR_DIR"/*.jar
do
  JAR_PATH="${JAR_PATH}:$(pwd)/entry"
done

CLASSPATH=$CLASSPATH:$JAR_PATH

echo $CLASSPATH