package edu.uob;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.stream.Stream;

public class DBServer {
    private final String storageFolderPath = "./data";
    private final Map<String, Path> databases = new ConcurrentHashMap<>();
    private final Map<String, Integer> tableIdCounters = new ConcurrentHashMap<>();
    private String currentDatabase = null;

    private static final String END_OF_TRANSMISSION = "EOT";
    private static final Logger LOGGER = Logger.getLogger(DBServer.class.getName());

    public DBServer() {
        setupLogger();
        loadExistingDatabases();
    }

    private void setupLogger() {
        try {
            FileHandler fileHandler = new FileHandler("DBServer.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fileHandler);
            LOGGER.setLevel(Level.INFO);
            LOGGER.info("Logger initialized.");
        } catch (IOException e) {
            System.err.println("Logger setup failed: " + e.getMessage());
        }
    }

    private void loadExistingDatabases() {
        LOGGER.info("Loading existing databases...");
        try (Stream<Path> paths = Files.walk(Paths.get(storageFolderPath))) {
            paths.filter(Files::isDirectory)
                    .forEach(path -> {
                        String dbName = path.getFileName().toString();
                        databases.put(dbName, path);
                        loadTablesForDatabase(dbName, path);
                    });
        } catch (IOException e) {
            LOGGER.severe("Error loading databases: " + e.getMessage());
        }
        LOGGER.info("Databases loaded successfully.");
    }

    private void loadTablesForDatabase(String dbName, Path dbPath) {
        LOGGER.info("Loading tables for database: " + dbName);
        try (Stream<Path> paths = Files.walk(dbPath)) {
            paths.filter(Files::isRegularFile)
                    .forEach(tablePath -> {
                        String tableName = tablePath.getFileName().toString().replace(".tsv", "");
                        initializeIdCounterForTable(tableName, tablePath);
                    });
        } catch (IOException e) {
            LOGGER.severe("Error loading tables for database " + dbName + ": " + e.getMessage());
        }
        LOGGER.info("Tables loaded successfully for database: " + dbName);
    }

    private void initializeIdCounterForTable(String tableName, Path tablePath) {
        try (Stream<String> lines = Files.lines(tablePath)) {
            OptionalInt maxId = lines.skip(1)
                    .filter(line -> !line.trim().isEmpty())
                    .mapToInt(line -> {
                        String[] parts = line.split("\t");
                        if (parts.length > 0) {
                            try {
                                return Integer.parseInt(parts[0]);
                            } catch (NumberFormatException e) {
                                LOGGER.severe("Invalid ID format in table " + tableName + ": " + e.getMessage());
                                return 0;
                            }
                        }
                        return 0;
                    })
                    .max();
            tableIdCounters.put(tableName, maxId.orElse(0) + 1);
        } catch (IOException e) {
            LOGGER.severe("Error initializing ID counter for table " + tableName + ": " + e.getMessage());
        }
    }

    public String handleCommand(String command) {
        LOGGER.info("Received command: " + command);
        String response;
        try {
            String[] commandParts = command.trim().split(" ", 2);
            if (commandParts.length < 2) {
                return "[ERROR] Invalid command";
            }
            switch (commandParts[0].toUpperCase()) {
                case "CREATE":
                    response = handleCreateCommand(commandParts[1]);
                    break;
                case "USE":
                    response = handleUseCommand(commandParts[1]);
                    break;
                case "INSERT":
                    response = handleInsertCommand(commandParts[1]);
                    break;
                case "SELECT":
                    response = handleSelectCommand(commandParts[1]);
                    break;
                default:
                    response = "[ERROR] Unknown command";
                    break;
            }
        } catch (Exception e) {
            LOGGER.severe("Error handling command: " + command + "; Error: " + e.getMessage());
            response = "[ERROR] " + e.getMessage();
        }
        LOGGER.info("Response: " + response);
        return response;
    }

    private String handleCreateCommand(String command) throws IOException {
        String[] parts = command.split(" ", 2);
        if ("DATABASE".equalsIgnoreCase(parts[0])) {
            return createDatabase(parts[1].replace(";", "").trim());
        } else if ("TABLE".equalsIgnoreCase(parts[0])) {
            return createTable(parts[1].replace(";", "").trim());
        } else {
            return "[ERROR] Invalid CREATE command";
        }
    }

    private String handleUseCommand(String command) {
        command = command.replace(";", "").trim();
        Path dbPath = databases.get(command);
        if (dbPath != null) {
            currentDatabase = command;
            return "[OK] Using " + command;
        } else {
            return "[ERROR] Database not found";
        }
    }

    private String handleInsertCommand(String command) throws IOException {
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        String[] parts = command.split("INTO|VALUES");
        if (parts.length != 3) {
            return "[ERROR] Invalid INSERT syntax";
        }
        String tableName = parts[1].trim();
        String values = parts[2].replace(";", "").trim();
        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableName + ".tsv");

        if (!Files.exists(tablePath)) {
            return "[ERROR] Table does not exist";
        }

        int newId;
        synchronized(this) {
            newId = tableIdCounters.getOrDefault(tableName, 1);
            try (BufferedWriter writer = Files.newBufferedWriter(tablePath, StandardOpenOption.APPEND)) {
                String[] valueParts = values.replaceAll("[()']", "").split(",");
                writer.write(newId + "\t" + String.join("\t", valueParts).trim());
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                System.err.println("Error writing to table: " + e.getMessage());
                return "[ERROR] Error writing to table";
            }
            tableIdCounters.put(tableName, newId + 1);
        }

        LOGGER.info("Insert command executed: " + command);
        return "[OK] Data inserted into table " + tableName;
    }

    private String handleSelectCommand(String command) throws IOException {
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }

        String[] parts = command.split("FROM");
        if (parts.length != 2) {
            return "[ERROR] Invalid SELECT syntax";
        }

        String selectClause = parts[0].trim().substring("SELECT".length()).trim();
        String tableName = parts[1].trim().split(" ")[0].trim();

        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableName + ".tsv");
        if (!Files.exists(tablePath)) {
            return "[ERROR] Table does not exist";
        }

        StringBuilder result = new StringBuilder();
        try (BufferedReader reader = Files.newBufferedReader(tablePath)) {
            String line;
            String[] headers = reader.readLine().split("\t");
            Map<String, Integer> columnIndices = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                columnIndices.put(headers[i], i);
            }

            while ((line = reader.readLine()) != null) {
                String[] rowData = line.split("\t");
                if ("*".equals(selectClause)) {
                    result.append(String.join("\t", rowData)).append("\n");
                } else {
                    for (String selectColumn : selectClause.split(",")) {
                        int columnIndex = columnIndices.getOrDefault(selectColumn.trim(), -1);
                        if (columnIndex != -1) {
                            result.append(rowData[columnIndex]).append("\t");
                        }
                    }
                    result.append("\n");
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading table data: " + e.getMessage());
            return "[ERROR] Error reading table data";
        }

        LOGGER.info("Select command executed: " + command);
        return result.length() > 0 ? "[OK]\n" + result.toString().trim() : "[ERROR] No data found";
    }

    private String createDatabase(String dbName) throws IOException {
        Path dbPath = Paths.get(storageFolderPath, dbName);
        if (Files.exists(dbPath)) {
            return "[ERROR] Database already exists";
        }
        Files.createDirectories(dbPath);
        databases.put(dbName, dbPath);
        return "[OK] Database " + dbName + " created";
    }

    private String createTable(String command) throws IOException {
        if (currentDatabase == null) {
            return "[ERROR] No database selected";
        }
        String[] parts = command.split(" ", 2);
        String tableName = parts[0];
        String columns = parts[1].replace("(", "").replace(")", "").replace(";", "").trim();

        Path tablePath = Paths.get(storageFolderPath, currentDatabase, tableName + ".tsv");
        if (Files.exists(tablePath)) {
            return "[ERROR] Table already exists";
        }
        try (BufferedWriter writer = Files.newBufferedWriter(tablePath, StandardOpenOption.CREATE)) {
            writer.write("id\t" + columns.replaceAll(",", "\t"));
            writer.newLine();
        }
        tableIdCounters.put(tableName, 1);
        return "[OK] Table " + tableName + " created";
    }

    private void blockingListenOn(int portNumber) throws IOException {
        LOGGER.info("Server starting, listening on port " + portNumber);
        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try (Socket clientSocket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                     BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

                    String command;
                    while ((command = in.readLine()) != null && !command.trim().isEmpty()) {
                        String result = handleCommand(command);
                        out.write(result);
                        out.write("\n" + END_OF_TRANSMISSION + "\n");
                        out.flush();
                    }
                } catch (IOException e) {
                    LOGGER.severe("Error processing client request: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            LOGGER.severe("Failed to start the server: " + e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        int portNumber = 8080;
        try {
            DBServer server = new DBServer();
            server.blockingListenOn(portNumber);
        } catch (IOException e) {
            System.err.println("Failed to start the server: " + e.getMessage());
        }
    }
}