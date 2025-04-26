package com.esad.Service;

import org.springframework.stereotype.Component;

@Component

public class Sidecar {

    public String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);
    }

    public String centerText(String text, int width) {
        int padding = (width - text.length()) / 2;
        return String.format("%" + (padding + text.length()) + "s", text);
    }

    public void printBox(String nodeId, NodeRole nodeRole, String coordinator, String title, String content) {
        String border = "═".repeat(60);
        System.out.println("\n╔" + border + "╗");
        System.out.println("║ " + centerText(title, 60) + " ║");
        System.out.println("╠" + "─".repeat(60) + "╣");
        System.out.println("║ " + padRight(content, 60) + " ║");
        System.out.println("║ " + padRight("Node: " + nodeId, 60) + " ║");
        System.out.println("║ " + padRight("Role: " + nodeRole, 60) + " ║");
        System.out.println("║ " + padRight("Coordinator: " + coordinator, 60) + " ║");
        System.out.println("╚" + border + "╝");
    }

    public void printMessage(String nodeId, NodeRole nodeRole, String msg) {
        System.out.println("[" + System.currentTimeMillis() + "][" + nodeId + "][" + nodeRole + "] " + msg);
    }
}