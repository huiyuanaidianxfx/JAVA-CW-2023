package edu.uob;

import java.util.ArrayList;
import java.util.List;

public class Player {
    private String name;
    private Location currentLocation;
    private List<Item> inventory;

    public Player(String name, Location startingLocation) {
        this.name = name;
        this.inventory = new ArrayList<>();
        this.currentLocation = startingLocation;
    }

    public String getName() {
        return name;
    }

    public Location getCurrentLocation() {
        return currentLocation;
    }

    public void setCurrentLocation(Location currentLocation) {
        this.currentLocation = currentLocation;
    }

    public List<Item> getInventory() {
        return inventory;
    }

    public void addItem(Item item) {
        inventory.add(item);
    }

    public void removeItem(Item item) {
        inventory.remove(item);
    }
}
