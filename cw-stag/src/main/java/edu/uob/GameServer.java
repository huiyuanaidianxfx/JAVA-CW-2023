package edu.uob;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.*;

public final class GameServer {
    private static final char END_OF_TRANSMISSION = 4;
    private HashMap<String, Location> locations;
    private HashMap<String, Player> players;
    private HashMap<String, HashSet<GameAction>> actions;

    public static void main(String[] args) {
        try {
            File entitiesFile = Paths.get("config" + File.separator + "basic-entities.dot").toAbsolutePath().toFile();
            File actionsFile = Paths.get("config" + File.separator + "basic-actions.xml").toAbsolutePath().toFile();
            GameServer server = new GameServer(entitiesFile, actionsFile);
            server.blockingListenOn(8888);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public GameServer(File entitiesFile, File actionsFile) {
        locations = new HashMap<>();
        players = new HashMap<>();
        actions = new HashMap<>();
        try {
            loadEntities(entitiesFile);
            loadActions(actionsFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadEntities(File file) throws IOException {
        // 初始化房间和家具
        Location cabin = new Location("Cabin", "A small log cabin.");
        cabin.addItem(new Item("potion", "A magic potion."));
        cabin.addFurniture(new Item("trapdoor", "A wooden trapdoor.")); // 添加家具
        locations.put(cabin.getName().toLowerCase(), cabin);

        Location forest = new Location("Forest", "A dark, spooky forest.");
        forest.addItem(new Item("key", "A small, rusty key."));
        locations.put(forest.getName().toLowerCase(), forest);
    }

    private void loadActions(File file) throws IOException {
    }

    public String handleCommand(String fullCommand) {
        String[] parts = fullCommand.split(":", 2);
        if (parts.length < 2) {
            return "Invalid command format";
        }
        String playerName = parts[0].trim();
        String command = parts[1].trim();
        return handleCommandInternal(playerName, command);
    }

    private String handleCommandInternal(String playerName, String command) {
        if (!players.containsKey(playerName)) {
            players.put(playerName, new Player(playerName, locations.get("cabin"))); // Default starting location
        }
        Player player = players.get(playerName);
        return processCommand(player, command);
    }

    private String processCommand(Player player, String command) {
        String[] parts = command.split(" ");
        String action = parts[0].toLowerCase();
        switch (action) {
            case "look":
                return look(player);
            case "get":
                return get(player, parts[1]);
            case "goto":
                return goTo(player, parts[1]);
            case "inv":
                return inventory(player);
            default:
                return "Unknown command";
        }
    }

    private String look(Player player) {
        Location location = player.getCurrentLocation();
        StringBuilder sb = new StringBuilder();
        sb.append(location.getName()).append("\n");
        sb.append(location.getDescription()).append("\n");

        for (Item item : location.getItems().values()) {
            sb.append(item.getName()).append(": ").append(item.getDescription()).append("\n");
        }

        for (Item furniture : location.getFurniture().values()) {
            sb.append(furniture.getName()).append(": ").append(furniture.getDescription()).append("\n");
        }

        for (String path : locations.keySet()) {
            sb.append("Path: ").append(path).append("\n");
        }

        return sb.toString();
    }

    private String get(Player player, String itemName) {
        Location location = player.getCurrentLocation();
        Item item = location.getItems().get(itemName.toLowerCase());
        if (item == null) {
            return "Item not found";
        }
        player.addItem(item);
        location.removeItem(itemName.toLowerCase());
        return "You picked up " + item.getName();
    }

    private String goTo(Player player, String locationName) {
        Location newLocation = locations.get(locationName.toLowerCase());
        if (newLocation == null) {
            return "Location not found";
        }
        player.setCurrentLocation(newLocation);
        return "You moved to " + newLocation.getName();
    }

    private String inventory(Player player) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are carrying:\n");
        for (Item item : player.getInventory()) {
            sb.append(item.getName()).append(": ").append(item.getDescription()).append("\n");
        }
        return sb.toString();
    }

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try (Socket clientSocket = serverSocket.accept();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
                    System.out.println("Connection established");
                    String fullCommand = reader.readLine();
                    if (fullCommand != null) {
                        System.out.println("Received command: " + fullCommand);
                        String result = handleCommand(fullCommand);
                        writer.write(result);
                        writer.write("\n" + END_OF_TRANSMISSION + "\n");
                        writer.flush();
                    }
                } catch (IOException e) {
                    System.out.println("Connection closed");
                }
            }
        }
    }
}
