### Create a Virtual Environment:

```bash
python -m venv .venv
```

### Activate the Virtual Environment:

- **Windows**

  ```bash
  .venv\Scripts\activate
  ```

- **macOS/Linux**
  ```bash
  . .venv/bin/activate
  ```

### Installing Dependencies:

```bash
# Install dependencies from requirements.txt in the root directory while in the virtual environment
pip install -r requirements.txt
```

### Download Required JAR Files:

```bash
# flink-connector-kafka
flink-connector-kafka-[version].jar

# kafka-clients
kafka-clients-[version].jar
```

**From**

- flink-connector-kafka: https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka

- kafka-clients: https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients

### Copy JAR Files to the Virtual Environment:

- **macOS/Linux:**

  ```bash
  mv path/to/flink-connector-kafka-[version].jar .venv/lib/pythonX.Y/site-packages/pyflink/lib/
  mv path/to/kafka-clients-[version].jar .venv/lib/pythonX.Y/site-packages/pyflink/lib/
  ```

- **Windows:**

  ```bash
  move path/to/flink-connector-kafka-[version].jar .venv\Lib\site-packages\pyflink\lib\
  move path/to/kafka-clients-[version].jar .venv\Lib\site-packages\pyflink\lib\
  ```

- **Directory Structure**
  ```bash
  project/
  │
  ├── .venv/
  │   ├── bin/
  │   ├── include/
  │   ├── lib/
  │   │   └── pythonX.Y/
  │   │       ├── site-packages/
  │   │       │   ├── pyflink/
  │   │       │   │   └── lib/
  │   │       │   │       ├── flink-connector-kafka-[version].jar
  │   │       │   │       └── kafka-clients-[version].jar
  │   └── share/
  ├── main.py
  └── requirements.txt
  ```
