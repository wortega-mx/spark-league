# Ranking List Console Application

## 1. Description

This project is a **Console Application** that reads an input file and generates a **ranking list** based on the processed data.

The application processes input data, applies ranking logic, and outputs a sorted ranking list directly in the console.

---

## 2. Tech Stack

- **Java SE 8**
- **Apache Spark (Java API)**
- **Maven** (build and dependency management)

---

## 3. How to Compile the Project (Generate JAR File)

This project uses **Maven** for building and packaging.

### Prerequisites

Make sure you have installed:

- Java 8 (JDK 1.8)
- Maven 3.x
- Proper environment variables configured:
  - `JAVA_HOME`
  - `MAVEN_HOME`

### Steps to Build

1. Open a terminal in the project root directory (where `pom.xml` is located).

2. Clean and compile the project:
   ```bash
   mvn clean compile

3. Run tests (optional):
   ```bash
   mvn test

4. Package the application into a JAR file:
   ```bash
   mvn clean package

5. After a successful build, the generated JAR file will be located in:
   target/

   Example:
   target/league-ranking-spark-1.0-SNAPSHOT.jar

## 4. How to Execute the JAR File

Once the JAR is generated, you can run it using the java -jar command.

As this JAR file expects a couple of arguments (file and output type), this is a real example:

```bash
java -jar target/league-ranking-spark-1.0-jar-with-dependencies.jar --input input/input.txt --output std

## 5. Notes

- Make sure all required dependencies (including Apache Spark) are properly included in the final JAR (use Maven Shade Plugin if needed).
- If Spark is running in local mode, no external cluster configuration is required.
- Verify Java 8 compatibility for all dependencies.

## Build Troubleshooting

If the build fails:
```bash
mvn clean install -U

Check dependency conflicts using:
```bash
mvn dependency:tree

