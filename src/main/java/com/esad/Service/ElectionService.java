package com.esad.Service;


import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


import static com.esad.Service.KafkaTopics.*;
import static com.esad.Service.Timeouts.*;
import static com.esad.Service.Utils.*;


@Service
@RequiredArgsConstructor
public class ElectionService {




    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    // Node state
    private final String nodeId = generateNodeId();
    private final AtomicReference<String> coordinator = new AtomicReference<>("❌");
    private final AtomicReference<NodeRole> nodeRole = new AtomicReference<>(NodeRole.UNASSIGNED);
    private volatile boolean electionInProgress = false;
    private volatile boolean awaitingHigherNodeResponse = false;
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    private final Set<String> knownNodes = Collections.synchronizedSet(new TreeSet<>());
    private final Map<Character, Integer> wordCounts = new ConcurrentHashMap<>();
    private final Map<Character, List<String>> wordExamples = new ConcurrentHashMap<>();
    private final Set<Character> assignedLetters = ConcurrentHashMap.newKeySet();


    // Scheduled tasks
    private ScheduledFuture<?> heartbeatFuture;
    private ScheduledFuture<?> heartbeatCheckFuture;
    private final ScheduledFuture<?>[] electionTimeoutFuture = new ScheduledFuture<?>[1];
    private ScheduledFuture<?> roleAssignmentFuture;
    private ScheduledFuture<?> documentProcessingFuture;


    @PostConstruct
    public void init() {
        knownNodes.add(nodeId);
        printBox("NODE INITIALIZED", "ID: " + nodeId + " | Role: " + nodeRole.get());
        startElection();
        startHeartbeatChecker();
    }


    @PreDestroy
    public void shutdown() {
        cancelScheduledTask(heartbeatFuture);
        cancelScheduledTask(heartbeatCheckFuture);
        cancelScheduledTask(electionTimeoutFuture[0]);
        cancelScheduledTask(roleAssignmentFuture);
        cancelScheduledTask(documentProcessingFuture);
        scheduler.shutdown();
    }


    private String generateNodeId() {
        return "node-" + new Random().nextInt(1000);
    }


    private void cancelScheduledTask(ScheduledFuture<?> task) {
        if (task != null && !task.isDone()) {
            task.cancel(true);
        }
    }


    // ========== Election Process ==========
    public synchronized void startElection() {
        if (electionInProgress || isCoordinator()) {
            return;
        }


        electionInProgress = true;
        awaitingHigherNodeResponse = true;
        printMessage("Starting election process");


        kafkaTemplate.send(ELECTION_TOPIC, "election", "ELECTION:" + nodeId);


        cancelScheduledTask(electionTimeoutFuture[0]);
        electionTimeoutFuture[0] = scheduler.schedule(() -> {
            synchronized (this) {
                if (awaitingHigherNodeResponse) {
                    printMessage("No higher nodes responded - declaring self as coordinator");
                    becomeCoordinator();
                }
                electionInProgress = false;
            }
        }, ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }


    private synchronized void becomeCoordinator() {
        coordinator.set(nodeId);
        nodeRole.set(NodeRole.COORDINATOR);
        kafkaTemplate.send(ELECTION_TOPIC, "election", "COORDINATOR:" + nodeId);
        printBox("COORDINATOR ELECTED", "Node " + nodeId + " is now coordinator");
        startHeartbeat();


        cancelScheduledTask(roleAssignmentFuture);
        roleAssignmentFuture = scheduler.schedule(this::assignRoles, ROLE_ASSIGNMENT_DELAY, TimeUnit.MILLISECONDS);
    }


    // ========== Role Assignment ==========
    private synchronized void assignRoles() {
        if (!isCoordinator()) {
            printMessage("Not coordinator - cannot assign roles");
            return;
        }

        List<String> nodes = new ArrayList<>(knownNodes);
        nodes.remove(nodeId);

        if (nodes.size() < 3) {
            printMessage("Not enough nodes for role assignment (need at least 3)");
            return;
        }


        // Assign learner (highest remaining ID)
        String learner = Collections.min(nodes);
        nodes.remove(learner);
        kafkaTemplate.send(ROLE_TOPIC, "role:LEARNER:" + learner);
        printMessage("Assigned LEARNER role to: " + learner);


        // Assign 2 proposers and remaining as acceptors
        Collections.sort(nodes, Collections.reverseOrder());
        List<String> proposers = nodes.subList(0, Math.min(2, nodes.size()));
        List<String> acceptors = nodes.subList(proposers.size(), nodes.size());


        proposers.forEach(node -> {
            kafkaTemplate.send(ROLE_TOPIC, "role:PROPOSER:" + node);
            printMessage("Assigned PROPOSER role to: " + node);
        });


        acceptors.forEach(node -> {
            kafkaTemplate.send(ROLE_TOPIC, "role:ACCEPTOR:" + node);
            printMessage("Assigned ACCEPTOR role to: " + node);
        });


        assignLetterRanges(proposers);


        printBox("ROLE ASSIGNMENT COMPLETE",
                "Learner: " + learner + "\n" +
                        "Proposers: " + proposers + "\n" +
                        "Acceptors: " + acceptors);


        // Start document processing after roles are assigned
        cancelScheduledTask(documentProcessingFuture);
        documentProcessingFuture = scheduler.schedule(() -> {
            printMessage("Starting document processing...");
            processDocument(DOCUMENT);
        }, DOCUMENT_PROCESSING_DELAY, TimeUnit.MILLISECONDS);
    }


    private void assignLetterRanges(List<String> proposers) {
        List<Character> letters = new ArrayList<>();
        for (char c = 'A'; c <= 'Z'; c++) {
            letters.add(c);
        }


        int lettersPerProposer = (int) Math.ceil(letters.size() / (double) proposers.size());


        for (int i = 0; i < proposers.size(); i++) {
            int start = i * lettersPerProposer;
            int end = Math.min(start + lettersPerProposer, letters.size());
            List<Character> range = letters.subList(start, end);


            String rangeStr = range.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));


            kafkaTemplate.send(ROLE_TOPIC, "range:" + proposers.get(i), rangeStr);
            printMessage("Assigned letters " + rangeStr + " to proposer " + proposers.get(i));
        }
    }


    // ========== Document Processing ==========
    private void processDocument(String document) {
        if (!isCoordinator()) {
            printMessage("Only coordinator can process documents");
            return;
        }


        printBox("DOCUMENT PROCESSING", "Starting document distribution");


        // Group words by their starting letter
        Map<Character, List<String>> wordsByLetter = Arrays.stream(document.split("\\s+"))
                .filter(word -> !word.isEmpty())
                .collect(Collectors.groupingBy(
                        word -> Character.toUpperCase(word.charAt(0)),
                        Collectors.toList()
                ));


        // Send words to their respective proposers
        wordsByLetter.forEach((letter, words) -> {
            String wordsStr = String.join(" ", words);
            kafkaTemplate.send(DOCUMENT_TOPIC, "words:" + letter, wordsStr);
            printMessage("Sent words for letter " + letter + " to proposers");
        });


        // Trigger counting after short delay
        scheduler.schedule(() -> {
            kafkaTemplate.send(COUNT_TOPIC, "trigger", "count");
            printMessage("Triggered counting process");
        }, 10000, TimeUnit.MILLISECONDS);
    }


    // ========== Message Handlers ==========
    @KafkaListener(topics = ELECTION_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleElectionMessage(String message) {
        printMessage("Received message: " + message);


        if (message.startsWith("ELECTION:")) {
            String candidateId = message.substring("ELECTION:".length());
            if (!knownNodes.contains(candidateId)) {
                knownNodes.add(candidateId);
                printMessage("Discovered new node: " + candidateId);
            }


            printMessage("Received ELECTION from " + candidateId);
            if (nodeId.compareTo(candidateId) > 0) {
                printMessage("I have higher priority - responding and starting new election");
                kafkaTemplate.send(ELECTION_TOPIC, "election", "OK:" + nodeId);
                startElection();
            } else {
                kafkaTemplate.send(ELECTION_TOPIC, "election", "OK:" + nodeId);
            }
        }
        else if (message.startsWith("OK:")) {
            String respondingNodeId = message.substring("OK:".length());
            printMessage("Received OK from " + respondingNodeId);


            synchronized (this) {
                if (electionInProgress && nodeId.compareTo(respondingNodeId) < 0) {
                    awaitingHigherNodeResponse = false;
                    cancelScheduledTask(electionTimeoutFuture[0]);
                }
            }
        }
        else if (message.startsWith("COORDINATOR:")) {
            String newCoordinator = message.substring("COORDINATOR:".length());
            if (!newCoordinator.equals(coordinator.get())) {
                coordinator.set(newCoordinator);
                electionInProgress = false;
                awaitingHigherNodeResponse = false;
                lastHeartbeatTime = System.currentTimeMillis();
                cancelScheduledTask(electionTimeoutFuture[0]);


                printBox("NEW COORDINATOR", "Node " + newCoordinator + " is now coordinator");


                // Reset role if we were coordinator
                if (nodeRole.get() == NodeRole.COORDINATOR) {
                    nodeRole.set(NodeRole.UNASSIGNED);
                    cancelScheduledTask(heartbeatFuture);
                    printMessage("Demoted from coordinator role");
                }
            }
        }
    }


    @KafkaListener(topics = ROLE_TOPIC , groupId = "${spring.kafka.consumer.group-id}")
    public void handleRoleAssignment(String message) {
        printMessage("Received role assignment: " + message);


        if (message.startsWith("role:LEARNER:")) {
            String learnerId = message.substring("role:LEARNER:".length());
            if (learnerId.equals(nodeId)) {
                nodeRole.set(NodeRole.LEARNER);
                printBox("ROLE ASSIGNED", "I am now LEARNER");
            }
        }
        else if (message.startsWith("role:PROPOSER:")) {
            String proposerId = message.substring("role:PROPOSER:".length());
            if (proposerId.equals(nodeId)) {
                nodeRole.set(NodeRole.PROPOSER);
                printBox("ROLE ASSIGNED", "I am now PROPOSER");
            }
        }
        else if (message.startsWith("role:ACCEPTOR:")) {
            String acceptorId = message.substring("role:ACCEPTOR:".length());
            if (acceptorId.equals(nodeId)) {
                nodeRole.set(NodeRole.ACCEPTOR);
                printBox("ROLE ASSIGNED", "I am now ACCEPTOR");
            }
        }
        else if (message.startsWith("range:")) {
            String[] parts = message.split(":");
            if (parts.length > 1 && nodeRole.get() == NodeRole.PROPOSER) {
                assignedLetters.clear();
                Arrays.stream(parts[1].split(","))
                        .forEach(c -> assignedLetters.add(c.charAt(0)));
                printBox("LETTER RANGES", "Assigned letters: " + parts[1]);
            }
        }
    }


    @KafkaListener(topics = DOCUMENT_TOPIC , groupId = "${spring.kafka.consumer.group-id}")
    public void handleDocumentWords(String message) {

        if (nodeRole.get() != NodeRole.PROPOSER) return;


        printMessage("Received document words: " + message);


        if (message.startsWith("words:")) {
            String[] parts = message.split(":");
            char letter = parts[1].charAt(0);
            if (assignedLetters.contains(letter)) {
                String[] words = parts[2].split(" ");
                wordCounts.merge(letter, words.length, Integer::sum);


                // Add examples (keep up to 3)
                List<String> examples = wordExamples.computeIfAbsent(letter, k -> new ArrayList<>());
                for (String word : words) {
                    if (examples.size() < 3 && !examples.contains(word)) {
                        examples.add(word);
                    }
                }
                printMessage("Processed " + words.length + " words for letter " + letter);
            }
        }
    }


    @KafkaListener(topics = COUNT_TOPIC , groupId = "${spring.kafka.consumer.group-id}")
    public void handleCountRequests(String request) {

        if (nodeRole.get() != NodeRole.PROPOSER) return;


        printMessage("Received count request: " + request);


        wordCounts.forEach((letter, count) -> {
            String examples = wordExamples.getOrDefault(letter, List.of())
                    .stream()
                    .collect(Collectors.joining(","));
            String message = String.format("%s:%d:%s", letter, count, examples);
            kafkaTemplate.send(VALIDATION_TOPIC, "count", message);
            printMessage("Sent count for letter " + letter + " to acceptors");
        });


        printMessage("Completed sending all counts for validation");
        wordCounts.clear();
        wordExamples.clear();
    }


    @KafkaListener(topics = VALIDATION_TOPIC , groupId = "${spring.kafka.consumer.group-id}")
    public void handleValidationRequests(String countData) {

        if (nodeRole.get() != NodeRole.ACCEPTOR) return;


        printMessage("Received validation request: " + countData);


        // Simple validation - just forward to learner
        kafkaTemplate.send(RESULT_TOPIC, "validated", countData);
        printMessage("Validated and forwarded count: " + countData.split(":")[0]);
    }


    @KafkaListener(topics = RESULT_TOPIC , groupId = "${spring.kafka.consumer.group-id}")
    public void handleFinalResults(String result) {
        if (nodeRole.get() != NodeRole.LEARNER) {
            printMessage("Not a learner - ignoring result message");
            return;
        }


        printMessage("Received final result: " + result);


        String[] parts = result.split(":");
        char letter = parts[0].charAt(0);
        int count = Integer.parseInt(parts[1]);
        String[] examples = parts.length > 2 ? parts[2].split(",") : new String[0];


        wordCounts.merge(letter, count, Integer::sum);


        // Add examples if we don't have enough yet
        List<String> currentExamples = wordExamples.computeIfAbsent(letter, k -> new ArrayList<>());
        for (String example : examples) {
            if (currentExamples.size() < 3 && !currentExamples.contains(example)) {
                currentExamples.add(example);
            }
        }


        // Print the final results box when we have all letters (A-Z)
        if (wordCounts.size() >= 26) {
            printFinalResults();
        }
    }


    // ========== Heartbeat System ==========
    private void startHeartbeat() {
        cancelScheduledTask(heartbeatFuture);
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            if (isCoordinator()) {
                kafkaTemplate.send(HEARTBEAT_TOPIC, "heartbeat", "HEARTBEAT:" + nodeId);
                printMessage("♥ Heartbeat sent");
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }


    private void startHeartbeatChecker() {
        cancelScheduledTask(heartbeatCheckFuture);
        heartbeatCheckFuture = scheduler.scheduleAtFixedRate(() -> {
            if (!isCoordinator() && !coordinator.get().equals("❌")) {
                long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatTime;
                if (timeSinceLastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                    printMessage("‼️ Coordinator failure detected - starting election");
                    coordinator.set("❌");
                    nodeRole.set(NodeRole.UNASSIGNED);
                    startElection();
                }
            }
        }, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMEOUT_MS/2, TimeUnit.MILLISECONDS);
    }


    @KafkaListener(topics = HEARTBEAT_TOPIC , groupId = "${spring.kafka.consumer.group-id}")
    public void handleHeartbeat(String message) {
        if (message.startsWith("HEARTBEAT:")) {
            String coordinatorId = message.substring("HEARTBEAT:".length());
            if (coordinatorId.equals(coordinator.get())) {
                lastHeartbeatTime = System.currentTimeMillis();
                printMessage("Received heartbeat from coordinator");
            }
        }
    }


    // ========== Final Results Display ==========
    private void printFinalResults() {
        StringBuilder content = new StringBuilder();


        // Sort letters alphabetically
        wordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    char letter = entry.getKey();
                    int count = entry.getValue();
                    String examples = wordExamples.getOrDefault(letter, List.of())
                            .stream()
                            .collect(Collectors.joining(","));


                    content.append(String.format("%s: %d words (%s)", letter, count, examples))
                            .append("\n");
                });


        String border = "═".repeat(58);
        System.out.println("\n╔" + border + "╗");
        System.out.println("║" + centerText("FINAL RESULTS", 58) + "║");
        System.out.println("╠" + "─".repeat(58) + "╣");


        // Split content into lines and format each line
        Arrays.stream(content.toString().split("\n"))
                .forEach(line -> {
                    String paddedLine = String.format("%-52s", line);
                    System.out.println("║ " + paddedLine + " ║");
                });


        System.out.println("║ " + padRight("Node: " + nodeId, 56) + " ║");
        System.out.println("║ " + padRight("Role: " + nodeRole.get(), 56) + " ║");
        System.out.println("║ " + padRight("Coordinator: " + coordinator.get(), 56) + " ║");
        System.out.println("╚" + border + "╝");
    }


    // ========== Utility Methods ==========
    private String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);
    }


    private String centerText(String text, int width) {
        int padding = (width - text.length()) / 2;
        return String.format("%" + (padding + text.length()) + "s", text);
    }


    private void printBox(String title, String content) {
        String border = "═".repeat(60);
        System.out.println("\n╔" + border + "╗");
        System.out.println("║ " + centerText(title, 60) + " ║");
        System.out.println("╠" + "─".repeat(60) + "╣");
        System.out.println("║ " + padRight(content, 60) + " ║");
        System.out.println("║ " + padRight("Node: " + nodeId, 60) + " ║");
        System.out.println("║ " + padRight("Role: " + nodeRole.get(), 60) + " ║");
        System.out.println("║ " + padRight("Coordinator: " + coordinator.get(), 60) + " ║");
        System.out.println("╚" + border + "╝");
    }


    private void printMessage(String msg) {
        System.out.println("[" + System.currentTimeMillis() + "][" + nodeId + "][" + nodeRole.get() + "] " + msg);
    }


    public boolean isCoordinator() {
        return coordinator.get().equals(nodeId);
    }
}

