package edu.uob;

import java.util.HashMap;
import java.util.Map;

public class Location {
    private String name;
    private String description;
    private Map<String, Item> items;
    private Map<String, Item> furniture;

    public Location(String name, String description) {
        this.name = name;
        this.description = description;
        this.items = new HashMap<>();
        this.furniture = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, Item> getItems() {
        return items;
    }

    public void addItem(Item item) {
        items.put(item.getName().toLowerCase(), item);
    }

    public void removeItem(String itemName) {
        items.remove(itemName.toLowerCase());
    }

    public Map<String, Item> getFurniture() {
        return furniture;
    }

    public void addFurniture(Item furnitureItem) {
        furniture.put(furnitureItem.getName().toLowerCase(), furnitureItem);
    }

    public void removeFurniture(String furnitureName) {
        furniture.remove(furnitureName.toLowerCase());
    }
}
