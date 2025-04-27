package com.esad.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


import java.util.*;
import java.util.concurrent.*;


import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.esad.Service.Timeouts.*;
import static com.esad.Service.Utils.DOCUMENT;


@Service
@RequiredArgsConstructor
public class ElectionService {
    private final Sidecar sidecar;
    static final String ELECTION_TOPIC = "election-topic";
    static final String HEARTBEAT_TOPIC = "heartbeat-topic";
    static final String ROLE_TOPIC = "role-assignment-topic";
    static final String DOCUMENT_TOPIC = "document-topic";
    static final String COUNT_TOPIC = "count-topic";
    static final String VALIDATION_TOPIC = "validation-topic";
    static final String RESULT_TOPIC = "result-topic";




    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);


    // Node state
    private final String nodeId = generateNodeId();
    private final AtomicReference<String> coordinator = new AtomicReference<>("❌");
    private final AtomicReference<NodeRole> nodeRole = new AtomicReference<>(NodeRole.CANDIDATE);
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
        sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "NODE INITIALIZED", "ID: " + nodeId + " | Role: " + nodeRole.get());
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

        sidecar.printMessage(nodeId, nodeRole.get(),"Starting election process");


        kafkaTemplate.send(ELECTION_TOPIC, "election", "ELECTION:" + nodeId);


        cancelScheduledTask(electionTimeoutFuture[0]);
        electionTimeoutFuture[0] = scheduler.schedule(() -> {
            synchronized (this) {
                if (awaitingHigherNodeResponse) {
                    sidecar.printMessage(nodeId, nodeRole.get(),"No higher nodes responded - declaring self as coordinator");

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
        sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "COORDINATOR ELECTED", "Node " + nodeId + " is now coordinator");
        startHeartbeat();


        cancelScheduledTask(roleAssignmentFuture);
        roleAssignmentFuture = scheduler.schedule(this::assignRoles, ROLE_ASSIGNMENT_DELAY, TimeUnit.MILLISECONDS);
    }


    private synchronized void assignRoles() {
        if (!isCoordinator()) {
            sidecar.printMessage(nodeId, nodeRole.get(),"Not coordinator - cannot assign roles");

            return;
        }


        List<String> nodes = new ArrayList<>(knownNodes);
        nodes.remove(nodeId);


        if (nodes.size() < 3) {
            sidecar.printMessage(nodeId, nodeRole.get(),"Not enough nodes for role assignment (need at least 3)");

            return;
        }


        // Shuffle nodes to distribute roles more evenly (simulating similar capability)
        Collections.shuffle(nodes);


        // Assign learner (select randomly instead of always taking the lowest ID)
        String learner = nodes.remove(0);
        kafkaTemplate.send(ROLE_TOPIC, "role:LEARNER:" + learner);

        sidecar.printMessage(nodeId, nodeRole.get(),"Assigned LEARNER role to: " + learner);



        // Calculate how many proposers we need (at least 1, at most 40% of remaining nodes)
        int totalRemaining = nodes.size();
        int proposerCount = Math.min(Math.max(1, (int) Math.ceil(totalRemaining * 0.4)), 2);
        int acceptorCount = totalRemaining - proposerCount;


        // Assign proposers and acceptors
        List<String> proposers = new ArrayList<>();
        List<String> acceptors = new ArrayList<>();


        // Alternate between assigning proposers and acceptors
        for (int i = 0; i < nodes.size(); i++) {
            String node = nodes.get(i);
            if (proposers.size() < proposerCount && (i % 2 == 0 || acceptors.size() >= acceptorCount)) {
                proposers.add(node);
                kafkaTemplate.send(ROLE_TOPIC, "role:PROPOSER:" + node);

                sidecar.printMessage(nodeId, nodeRole.get(),"Assigned PROPOSER role to: " + node);
            } else {
                acceptors.add(node);
                kafkaTemplate.send(ROLE_TOPIC, "role:ACCEPTOR:" + node);
                sidecar.printMessage(nodeId, nodeRole.get(),"Assigned ACCEPTOR role to: " + node);

            }
        }


        assignLetterRanges(proposers);



        sidecar.printBox(
                nodeId,
                nodeRole.get(),
                coordinator.get(),
                "ROLE ASSIGNMENT COMPLETE",
                "Learner: " + learner + "\n" +
                        "Proposers: " + proposers + " (" + proposers.size() + ")\n" +
                        "Acceptors: " + acceptors + " (" + acceptors.size() + ")"
        );
        // Start document processing after roles are assigned
        cancelScheduledTask(documentProcessingFuture);
        documentProcessingFuture = scheduler.schedule(() -> {
            sidecar.printMessage(nodeId, nodeRole.get(),"Starting document processing...");

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


            String rangeStr = range.get(0) + "-" + range.get(range.size()-1);


            //  kafkaTemplate.send(ROLE_TOPIC, "range:" + proposers.get(i), rangeStr);
            String rangeMessage = "range:" + proposers.get(i) + ":" + rangeStr;
            kafkaTemplate.send(ROLE_TOPIC, rangeMessage);

            sidecar.printMessage(nodeId, nodeRole.get(),"Assigned letters " + rangeStr + " to proposer " + proposers.get(i));

        }
    }


    // ========== Document Processing ==========
    private void processDocument(String document) {
        if (!isCoordinator()) {
            sidecar.printMessage(nodeId, nodeRole.get(),"Only coordinator can process documents");

            return;
        }


        sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "DOCUMENT PROCESSING", "Starting document distribution");


        // Clear previous counts before processing new document
        wordCounts.clear();
        wordExamples.clear();


        // Process document line by line
        Arrays.stream(document.split("\n"))
                .forEach(line -> {
                    // Filter out empty lines and split into words
                    String[] words = line.split("\\s+");


                    // Group words by their starting letter (case-insensitive)
                    Map<Character, List<String>> wordsByLetter = Arrays.stream(words)
                            .filter(word -> !word.isEmpty())
                            .collect(Collectors.groupingBy(
                                    word -> Character.toUpperCase(word.charAt(0)),
                                    Collectors.toList()
                            ));


                    // Send words to their respective proposers
                    wordsByLetter.forEach((letter, wordsList) -> {
                        String wordsStr = String.join(" ", wordsList);
                        kafkaTemplate.send(DOCUMENT_TOPIC, "words:" + letter, wordsStr);

                        sidecar.printMessage(nodeId, nodeRole.get(),"Sent " + wordsList.size() + " words for letter " + letter);
                    });
                });


        // Trigger counting after short delay
        scheduler.schedule(() -> {
            kafkaTemplate.send(COUNT_TOPIC, "trigger", "count");

            sidecar.printMessage(nodeId, nodeRole.get(),"Triggered counting process");
        }, COUNT_TRIGGER_DELAY, TimeUnit.MILLISECONDS);
    }


    // ========== Message Handlers ==========
    @KafkaListener(topics = ELECTION_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleElectionMessage(String message) {


        sidecar.printMessage(nodeId, nodeRole.get(),"Received message: " + message);
        if (message.startsWith("ELECTION:")) {
            String candidateId = message.substring("ELECTION:".length());
            if (!knownNodes.contains(candidateId)) {
                knownNodes.add(candidateId);

                sidecar.printMessage(nodeId, nodeRole.get(),"Discovered new node: " + candidateId);

            }

            sidecar.printMessage(nodeId, nodeRole.get(),"Received ELECTION from " + candidateId);
            if (nodeId.compareTo(candidateId) > 0) {

                sidecar.printMessage(nodeId, nodeRole.get(),"I have higher priority - responding and starting new election" ) ;
                kafkaTemplate.send(ELECTION_TOPIC, "election", "OK:" + nodeId);
                startElection();
            } else {
                kafkaTemplate.send(ELECTION_TOPIC, "election", "OK:" + nodeId);
            }
        }
        else if (message.startsWith("OK:")) {
            String respondingNodeId = message.substring("OK:".length());
            sidecar.printMessage(nodeId, nodeRole.get(),"Received OK from " + respondingNodeId);

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

                sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "NEW COORDINATOR", "Node " + newCoordinator + " is now coordinator");

                // Reset role if we were coordinator
                if (nodeRole.get() == NodeRole.COORDINATOR) {
                    nodeRole.set(NodeRole.CANDIDATE);
                    cancelScheduledTask(heartbeatFuture);

                    sidecar.printMessage(nodeId, nodeRole.get(),"Demoted from coordinator role");             }
            }
        }
    }


    @KafkaListener(topics = ROLE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleRoleAssignment(String message) {


        sidecar.printMessage(nodeId, nodeRole.get(),"Received role assignment: " + message);
        if (message.startsWith("role:LEARNER:")) {
            String learnerId = message.substring("role:LEARNER:".length());
            if (learnerId.equals(nodeId)) {
                nodeRole.set(NodeRole.LEARNER);
                sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "ROLE ASSIGNED", "I am now LEARNER");
            }
        }
        else if (message.startsWith("role:PROPOSER:")) {
            String proposerId = message.substring("role:PROPOSER:".length());
            if (proposerId.equals(nodeId)) {
                nodeRole.set(NodeRole.PROPOSER);
                sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "ROLE ASSIGNED", "I am now PROPOSER");

            }
        }
        else if (message.startsWith("role:ACCEPTOR:")) {
            String acceptorId = message.substring("role:ACCEPTOR:".length());
            if (acceptorId.equals(nodeId)) {
                nodeRole.set(NodeRole.ACCEPTOR);
                sidecar.printBox(nodeId, nodeRole.get(), coordinator.get(), "ROLE ASSIGNED", "I am now ACCEPTOR");


            }
        }else if (message.startsWith("range:")) {
            String[] parts = message.split(":");
            if (parts.length == 3) {
                String targetNodeId = parts[1];
                String rangeStr = parts[2];


                if (targetNodeId.equals(nodeId) && nodeRole.get() == NodeRole.PROPOSER) {
                    assignedLetters.clear();
                    String[] rangeParts = rangeStr.split("-");
                    char start = rangeParts[0].charAt(0);
                    char end = rangeParts[1].charAt(0);


                    for (char c = start; c <= end; c++) {
                        assignedLetters.add(c);
                    }
                    sidecar.printBox(
                            nodeId,
                            nodeRole.get(),
                            coordinator.get(),
                            "LETTER RANGES",
                            "Assigned letters: " + rangeStr
                    );

                }
            }
        }


    }


    @KafkaListener(topics = DOCUMENT_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleDocumentWords(ConsumerRecord<String, String> record) {
        if (nodeRole.get() != NodeRole.PROPOSER) return;


        String key = record.key();      // "words:A"
        String value = record.value();  // "Apple Ant Axis"


        if (key != null && key.startsWith("words:")) {
            char letter = key.charAt(6);  // e.g., 'A'
            if (assignedLetters.contains(letter)) {
                // Split words properly, handling multiple spaces
                String[] words = value.trim().split("\\s+");
                int validWordCount = 0;


                for (String word : words) {
                    if (!word.isEmpty()) {
                        validWordCount++;
                        // Add examples (keep up to 3)
                        List<String> examples = wordExamples.computeIfAbsent(letter, k -> new ArrayList<>());
                        if (examples.size() < 3 && !examples.contains(word)) {
                            examples.add(word);
                        }
                    }
                }


                wordCounts.merge(letter, validWordCount, Integer::sum);
                sidecar.printMessage(nodeId, nodeRole.get(),"Processed " + validWordCount + " valid words for letter " + letter);

            }
        }
    }

    @KafkaListener(topics = COUNT_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleCountRequests(String request) {
        if (nodeRole.get() != NodeRole.PROPOSER) return;


        sidecar.printMessage(nodeId, nodeRole.get(),"Received count request: " + request);

        // Report counts for all assigned letters
        assignedLetters.forEach(letter -> {
            int count = wordCounts.getOrDefault(letter, 0);
            String examples = wordExamples.getOrDefault(letter, List.of())
                    .stream()
                    .limit(3)  // Ensure we only send up to 3 examples
                    .collect(Collectors.joining(","));


            String message = String.format("%s:%d:%s", letter, count, examples);
            kafkaTemplate.send(VALIDATION_TOPIC, "count", message);

            sidecar.printMessage(nodeId, nodeRole.get(),"Sent count for letter " + letter + ": " + count + " words");
        });


        // Clear counts after reporting (optional)
        wordCounts.clear();
        wordExamples.clear();
    }
    @KafkaListener(topics = VALIDATION_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleValidationRequests(String countData) {
        if (nodeRole.get() != NodeRole.ACCEPTOR) return;

        sidecar.printMessage(nodeId, nodeRole.get(),"Received validation request: " + countData);


    // 1. Split into components: letter:count[:examples]
    String[] parts = countData.split(":", 3);
    if (parts.length < 2) {
        sidecar.printMessage(nodeId, nodeRole.get(), "Invalid format, dropping: " + countData);
        return;
    }

    String letterPart = parts[0];
    String countPart = parts[1];
    String examplesPart = parts.length == 3 ? parts[2] : "";

    // 2. Validate letter is a single A–Z character
    if (letterPart.length() != 1 || !Character.isLetter(letterPart.charAt(0))) {
        sidecar.printMessage(nodeId, nodeRole.get(), "Invalid letter, dropping: " + letterPart);
        return;
    }
    char letter = Character.toUpperCase(letterPart.charAt(0));

    // 3. Validate count is a non-negative integer
    int count;
    try {
        count = Integer.parseInt(countPart);
        if (count < 0) {
            sidecar.printMessage(nodeId, nodeRole.get(), "Negative count, dropping: " + countPart);
            return;
        }
    } catch (NumberFormatException e) {
        sidecar.printMessage(nodeId, nodeRole.get(), "Malformed count, dropping: " + countPart);
        return;
    }

    // 4. (Optional) Validate examples: at most 3, and none are empty
    if (!examplesPart.isEmpty()) {
        String[] examples = examplesPart.split(",");
        if (examples.length > 3) {
            sidecar.printMessage(nodeId, nodeRole.get(), "Too many examples, trimming to 3");
            // trim down to first 3
            examplesPart = String.join(",", Arrays.copyOf(examples, 3));
        }
        for (String ex : examplesPart.split(",")) {
            if (ex.isBlank()) {
                sidecar.printMessage(nodeId, nodeRole.get(), "Empty example found, dropping message");
                return;
            }
        }
    }
        kafkaTemplate.send(RESULT_TOPIC, "validated", countData);

        sidecar.printMessage(nodeId, nodeRole.get(),"Validated and forwarded count: " + countData.split(":")[0]);

    }








    @KafkaListener(topics = RESULT_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleFinalResult(String result) {
        if (nodeRole.get() != NodeRole.LEARNER) {
            return;
        }


        sidecar.printMessage(nodeId, nodeRole.get(),"Received final result: " + result);

        String[] parts = result.split(":");
        if (parts.length < 2) {

            sidecar.printMessage(nodeId, nodeRole.get(),"Invalid result format: " + result);
            return;
        }


        try {
            char letter = parts[0].charAt(0);
            int count = Integer.parseInt(parts[1]);
            String[] examples = (parts.length == 3) ?
                    parts[2].split(",", 3) :  // Limit to 3 examples
                    new String[0];


            // Only update if this is a new count (not a duplicate)
            if (!wordCounts.containsKey(letter)) {
                wordCounts.put(letter, count);


                // Add examples if we don't have any yet
                if (!wordExamples.containsKey(letter)) {
                    List<String> exampleList = new ArrayList<>();
                    for (String example : examples) {
                        if (!example.isEmpty() && exampleList.size() < 3) {
                            exampleList.add(example);
                        }
                    }
                    wordExamples.put(letter, exampleList);
                }
            }


            // Check if we have all letters
            if (wordCounts.size() >= 26) {
                printFinalResults();
            }
        } catch (Exception e) {

            sidecar.printMessage(nodeId, nodeRole.get(),"Error processing result: " + e.getMessage());
        }
    }
    private synchronized void printFinalResults() {
        // Ensure we have all letters A-Z
        for (char c = 'A'; c <= 'Z'; c++) {
            wordCounts.putIfAbsent(c, 0);
        }


        // Build the results table
        StringBuilder results = new StringBuilder();
        results.append("\n╔══════════════════════════════════════════════════════════╗\n");
        results.append("║                 FINAL WORD COUNT RESULTS                 ║\n");
        results.append("╠══════════════════════════════════════════════════════════╣\n");
        results.append("║ Letter   Count   Example Words                          ║\n");
        results.append("╠══════════════════════════════════════════════════════════╣\n");


        // Use arrays to work around the effectively final requirement
        int[] totalWords = {0};
        int[] lettersWithWords = {0};


        // Sort letters alphabetically
        wordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    char letter = entry.getKey();
                    int count = entry.getValue();
                    totalWords[0] += count;


                    List<String> examples = wordExamples.getOrDefault(letter, Collections.emptyList());
                    String examplesStr = examples.isEmpty() ? "None" :
                            examples.stream().limit(3).collect(Collectors.joining(", "));


                    if (count > 0) {
                        lettersWithWords[0]++;
                    }


                    results.append(String.format("║   %s      %-6d %-36s ║\n",
                            letter, count, examplesStr));
                });


        // Add summary
        results.append("╠══════════════════════════════════════════════════════════╣\n");
        results.append(String.format("║ Letters with words: %d/26%45s ║\n", lettersWithWords[0], ""));
        results.append(String.format("║ Total words counted: %d%43s ║\n", totalWords[0], ""));
        results.append(String.format("║ Node: %-50s ║\n", nodeId));
        results.append(String.format("║ Role: %-49s ║\n", nodeRole.get()));
        results.append(String.format("║ Coordinator: %-44s ║\n", coordinator.get()));
        results.append("╚══════════════════════════════════════════════════════════╝\n");


        System.out.println(results.toString());
    }


    // ========== Heartbeat System ==========
    private void startHeartbeat() {
        cancelScheduledTask(heartbeatFuture);
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            if (isCoordinator()) {
                kafkaTemplate.send(HEARTBEAT_TOPIC, "heartbeat", "HEARTBEAT:" + nodeId);

                sidecar.printMessage(nodeId, nodeRole.get(),"♥ Heartbeat sent");

            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void startHeartbeatChecker() {
        cancelScheduledTask(heartbeatCheckFuture);
        heartbeatCheckFuture = scheduler.scheduleAtFixedRate(() -> {
            if (!isCoordinator() && !coordinator.get().equals("❌")) {
                long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatTime;
                if (timeSinceLastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                    sidecar.printMessage(nodeId, nodeRole.get(),"‼️ Coordinator failure detected - starting election");

                    coordinator.set("❌");
                    nodeRole.set(NodeRole.CANDIDATE);
                    startElection();
                }
            }
        }, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMEOUT_MS/2, TimeUnit.MILLISECONDS);
    }

    @KafkaListener(topics = HEARTBEAT_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void handleHeartbeat(String message) {
        if (message.startsWith("HEARTBEAT:")) {
            String coordinatorId = message.substring("HEARTBEAT:".length());
            if (coordinatorId.equals(coordinator.get())) {
                lastHeartbeatTime = System.currentTimeMillis();
                sidecar.printMessage(nodeId, nodeRole.get(),"Received heartbeat from coordinator");
            }
        }
    }

    public boolean isCoordinator() {
        return coordinator.get().equals(nodeId);
    }




}

