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

### Required JAR Files for Presence Detection Script:

1. kafka-clients-x.y.z.jar
   - Download from: [kafka-clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients)
1. flink-connector-kafka-x.y.z.jar
   - Download from: [flink-connector-kafka](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka)

These files should be placed in the directory:

`.venv/lib/python3.x/site-packages/pyflink/lib/`
