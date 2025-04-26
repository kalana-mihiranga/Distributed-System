package com.esad.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {

    static final String DOCUMENT;

    static {
        String documentContent = "";
        try {
            Path path = Paths.get(Utils.class.getClassLoader().getResource("document.txt").toURI());
            documentContent = Files.readString(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        DOCUMENT = documentContent;
    }



}
