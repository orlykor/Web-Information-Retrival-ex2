package webdata;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;


public class IndexWriter {
    private static final String SORTED_FILE = "sortedPairs";
    private static final String PRODUCT_ID_SORTED_FILE = "ProductIdSortedPairs";
    private static final String REGEX = "\\w+/\\w+:\\s*";
    private static final int BLOCK_SIZE = 4096;
    private static final int NUM_OF_THREADS = 4;
    private ReviewsCollection reviews;
    private Dictionary dictionary;
    private Dictionary productIdsDict;
    static ArrayList<String> tokens;
    static ArrayList<String> productIdTokens;
    private HashSet<String> hashedTokens = new HashSet<>();
    private HashSet<String> hashedProductIds = new HashSet<>();

    private int[] reviewLength = new int[NUM_OF_THREADS];
    private ReentrantLock hashTokenLock= new ReentrantLock();
    private ReentrantLock hashedProductIdsLock = new ReentrantLock();
    private ReentrantLock fileIndexLock = new ReentrantLock();
    private ReentrantLock productIDFileIndexLock = new ReentrantLock();
    private int productIDFileIndex = 0;
    private int fileIndex = 0;


    final Comparator<int[]> pairsComparator = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            int res = o1[0] - o2[0];
            if (res == 0) {
                return o1[1] - o2[1];
            }
            return res;
        }
    };


    /**
     * Given product review data, creates an on disk index
     * inputFile is the path to the file containing the review data
     * dir is the directory in which all index files will be created
     *     if the directory does not exist, it should be created
     */
    public void write(String inputFile, String dir) {
        /* for the dictionaries*/

        /*creating the sorted dictionaries, and the reviews collection*/
        try {
            File f = new File(inputFile);
            long size = f.length();

            ExecutorService firstParserPool = Executors.newFixedThreadPool(NUM_OF_THREADS);
            IntStream.range(0, NUM_OF_THREADS).forEach(
                 i-> firstParserPool.execute(
                         () -> {
                             try {
                                 firstParser(inputFile, (long)Math.ceil(i / (double)NUM_OF_THREADS * size), (long)Math.ceil((i + 1) / (double)NUM_OF_THREADS * size), i);
                             } catch (IOException e) {
                                 System.err.println("IOException error");
                                 System.exit(1);
                             }
                         }
            ));

            firstParserPool.shutdown();
            firstParserPool.awaitTermination(100, TimeUnit.MINUTES);

            tokens = new ArrayList<>(hashedTokens);
            Collections.sort(tokens);
            hashedTokens = null;
            productIdTokens = new ArrayList<>(hashedProductIds);
            Collections.sort(productIdTokens);
            hashedProductIds = null;
            reviews = new ReviewsCollection(IntStream.of(reviewLength).sum());

            InvertedIndex invertedIndex = new InvertedIndex();
            InvertedIndex productIdInvertedIndex = new InvertedIndex();

            File directory = new File(dir);
            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    System.err.println("Failed to create directory " + dir);
                    System.exit(1);
                }
            }
            ExecutorService secondParserPool = Executors.newFixedThreadPool(NUM_OF_THREADS);

            int sum = 1;
            for (int i=0; i < NUM_OF_THREADS; i++){
                int index = i;
                int startReviewId = sum;
                secondParserPool.execute(
                        () -> {
                            try {
                                secondParser(inputFile, (long)Math.ceil(index / (double)NUM_OF_THREADS * size), (long)Math.ceil((index + 1) /
                                        (double)NUM_OF_THREADS * size), dir, startReviewId);
                            } catch (IOException e) {
                                System.err.println("IOException error");
                                System.exit(1);
                            }
                        }
                );
                sum += reviewLength[index];
            }

            secondParserPool.shutdown();
            secondParserPool.awaitTermination(100, TimeUnit.MINUTES);

            merge(dir, PRODUCT_ID_SORTED_FILE, Consts.PRODUCT_ID_MERGE_B, true);
            merge(dir, SORTED_FILE, Consts.MERGE_B, false);

            dictionary = new Dictionary(Consts.DICTIONARY_K, tokens.size());
            productIdsDict = new Dictionary(Consts.DICT_PRODUCT_ID_K, productIdTokens.size());

            invertedIndex.writeObject(dictionary, Consts.INVERTED_INDEX_OBJ_FILE, dir, SORTED_FILE, false, Consts.B);
            productIdInvertedIndex.writeObject(productIdsDict, Consts.INVERTED_INDEX_PRODUCT_OBJ_FILE, dir,
                    PRODUCT_ID_SORTED_FILE, true, Consts.PRODUCT_ID_B);

            dictionary.writeObject(Consts.DICT_TABLE_OBJ_FILE, Consts.DICT_STR_OBJ_FILE, dir, false);
            productIdsDict.writeObject(Consts.DICT_TABLE_PROD_OBJ_FILE, Consts.DICT_STR_PROD_OBJ_FILE, dir, true);

            reviews.writeObject(Consts.REVIEWS_OBJ_FILE, dir);

        }
        catch (IOException e){
            System.err.println("IO Exception error");
            System.exit(1);

        } catch (InterruptedException e) {
            System.err.println("InterruptedException error");
            System.exit(1);
        }

    }


    /**
     * Delete all index files by removing the given directory
     */
    public void removeIndex(String dir) {
        File directory = new File(dir);
        File[] files = directory.listFiles();
        if (files != null){
            for(File file: files){
                if (!file.delete()){

                    System.err.println("Failed to delete file " + file.getName());
                    System.exit(1);
                }
            }
        }
        if (!directory.delete()){
            System.err.println("Failed to delete directory " + directory.getName());
            System.exit(1);

        }
    }


    private void firstParser(String inputFile, long from, long to, int i)throws IOException {

        String currLine = "", productId;
        String[] tokens;
        File f = new File(inputFile);
        long currOffset = from;
        RandomAccessFile reader = new RandomAccessFile(f, "r");
        String temp = reader.readLine();
        int newLine = (int)reader.getFilePointer() - temp.length();

        int min = (int)Math.min(f.length()/NUM_OF_THREADS, Consts.B * BLOCK_SIZE/NUM_OF_THREADS);
        reader.seek(from);
        FileReader fr = new FileReader(reader.getFD());
        BufferedReader br = new BufferedReader(fr, min);


        while (!currLine.startsWith("product/productId")){
            currLine  = br.readLine();
            currOffset += currLine.length() + newLine;
        }
        //every run of this while is of one review
        while (currLine != null && currOffset < to){
            reviewLength[i] ++;

            productId = currLine.replaceFirst(REGEX,"").toLowerCase();
            hashedProductIdsLock.lock();
            try{
                hashedProductIds.add(productId);
            } finally {
                hashedProductIdsLock.unlock();
            }
            //this line is the userId
            while (!currLine.startsWith("review/text")){
                currLine  = br.readLine();
                currOffset += currLine.length() + newLine;

            }
            //now currLine is of the text
            while(!currLine.startsWith("product/productId")){
                if (currLine.equals("")){
                    currLine = br.readLine();
                    if (currLine == null){
                        break;
                    }
                    int length = currLine.length() + newLine;
                    if (currOffset + length < to) {
                        currOffset += length;
                    }

                    continue;
                }
                currLine = currLine.replaceFirst(REGEX,"").toLowerCase();
                tokens = currLine.split("[\\W|_]+");
                hashTokenLock.lock();
                try {
                    for (String token: tokens) {
                        if (!token.equals("")){
                            if (token.length() > 256){
                                token = token.substring(0, 256);
                            }
                            hashedTokens.add(token);
                        }
                    }
                }finally {
                    hashTokenLock.unlock();
                }
                currLine = br.readLine();
                currOffset += currLine.length() + newLine;
            }
        }
        br.close();
    }


    private void sortAndWriteBlock(int[][] pairs, boolean isProductId, String dir) throws IOException, InterruptedException {
        BufferedOutputStream sortedBlock;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        int[][] first = Arrays.copyOfRange(pairs, 0, pairs.length/2);
        int[][] last = Arrays.copyOfRange(pairs, pairs.length/2, pairs.length);

        ByteBuffer byteBuffer = ByteBuffer.allocate(pairs.length * 8);
        if (isProductId){
            productIDFileIndexLock.lock();
            try {
                sortedBlock = new BufferedOutputStream(new FileOutputStream(dir+ File.separator + PRODUCT_ID_SORTED_FILE
                        + productIDFileIndex));
                productIDFileIndex++;
            }finally {
                productIDFileIndexLock.unlock();
            }
        }
        else{
            fileIndexLock.lock();
            try {
                sortedBlock = new BufferedOutputStream(new FileOutputStream(dir+ File.separator +SORTED_FILE+fileIndex));
                fileIndex++;
            }finally {
                fileIndexLock.unlock();
            }
        }
        pool.execute(
                () -> {
                    Arrays.sort(first, pairsComparator);
                }
        );
        pool.execute(
                () -> {
                    Arrays.sort(last, pairsComparator);
                }
        );
        pool.shutdown();
        pool.awaitTermination(100, TimeUnit.MINUTES);
        int i=0, j=0;
        while (i < first.length || j < last.length){
            if (j == last.length || (i != first.length && pairsComparator.compare(first[i], last[j]) < 0)){
                byteBuffer.putInt(first[i][0]);
                byteBuffer.putInt(first[i][1]);
                i++;
            }
            else {
                byteBuffer.putInt(last[j][0]);
                byteBuffer.putInt(last[j][1]);
                j++;
            }
        }
        sortedBlock.write(byteBuffer.array());
        pairs = null;
        byteBuffer = null;
        sortedBlock.close();
        sortedBlock = null;
    }

    /**
     * pareser for creating the inverted index - for each token create its review list.
     * @param inputFile
     * @throws IOException
     */
    private void secondParser(String inputFile, long from, long to, String dir, int reviewId) throws IOException {
        int tokenId, productIdCounter = 0, amountOfPairs = 0;
        int productIdIndex;
        byte score;
        short helpfulnessNumerator, helpfulnessDenominator, lengthReview = 0;
        String[] tokens, helpfulness;
        String productId, currLine = "";

        int[][] pairs = new int[BLOCK_SIZE * Consts.B / (8 * NUM_OF_THREADS)][2];
        int[][] productIDPairs = new int[BLOCK_SIZE * Consts.PRODUCT_ID_B / (8*NUM_OF_THREADS)][2];
        long currOffset = from;

        try {
            File f = new File(inputFile);
            RandomAccessFile reader = new RandomAccessFile(f, "r");
            String temp = reader.readLine();
            int newLine = (int)reader.getFilePointer() - temp.length();

            int min = (int)Math.min(f.length()/NUM_OF_THREADS, Consts.B * BLOCK_SIZE/NUM_OF_THREADS);
            reader.seek(from);
            FileReader fr = new FileReader(reader.getFD());
            BufferedReader br = new BufferedReader(fr, min);

            while (!currLine.startsWith("product/productId")){
                currLine  = br.readLine();
                currOffset += currLine.length() + newLine;
            }

            while (currLine != null && currOffset < to) {
                productId = currLine.replaceFirst(REGEX,"").toLowerCase();
                if (productIdCounter == BLOCK_SIZE * Consts.PRODUCT_ID_B/ (8 * NUM_OF_THREADS)){
                    this.sortAndWriteBlock(productIDPairs, true, dir);
                    productIdCounter = 0;
                }

                productIdIndex = Collections.binarySearch(productIdTokens, productId);
                productIDPairs[productIdCounter][0] = productIdIndex;
                productIDPairs[productIdCounter][1] = reviewId;
                productIdCounter++;

                while (!currLine.startsWith("review/helpfulness")){
                    currLine  = br.readLine();
                    currOffset += currLine.length() + newLine;

                }
                //after the while ends the currLine contains the helpfulness line
                helpfulness = currLine.replaceFirst(REGEX,"").split("/");
                helpfulnessNumerator = Short.parseShort(helpfulness[0]);
                helpfulnessDenominator = Short.parseShort(helpfulness[1]);
                currLine = br.readLine();
                score = (byte)Double.parseDouble(currLine.replaceFirst(REGEX,""));
                currOffset += currLine.length() + newLine;

                while(!currLine.startsWith("review/text:")){
                    currLine = br.readLine();
                    currOffset += currLine.length() + newLine;

                }
                while (!currLine.startsWith("product/productId")){
                    if (currLine.equals("")){
                        currLine = br.readLine();
                        if (currLine == null){
                            break;
                        }
                        int length = currLine.length() + newLine;
                        if (currOffset + length < to){
                            currOffset += length;
                        }
                        continue;
                    }
                    currLine = currLine.replaceFirst(REGEX,"").toLowerCase();
                    tokens = currLine.split("[\\W|_]+");
                    for (String token: tokens) {
                        if (!token.equals("")){
                            if(token.length() > 256){
                                token = token.substring(0, 256);
                            }
                            if (amountOfPairs == BLOCK_SIZE * Consts.B / (8 * NUM_OF_THREADS)){
                                this.sortAndWriteBlock(pairs, false, dir);
                                amountOfPairs = 0;
                            }
                            tokenId = Collections.binarySearch(IndexWriter.tokens, token);
                            pairs[amountOfPairs][0] = tokenId;
                            pairs[amountOfPairs][1] = reviewId;
                            amountOfPairs++;
                            lengthReview++;
                        }
                    }
                    currLine = br.readLine();
                    currOffset += currLine.length() + newLine;

                }
                this.reviews.addReview(productIdIndex, helpfulnessNumerator, helpfulnessDenominator, score, lengthReview, reviewId-1);
                lengthReview = 0;
                reviewId++;
            }
            //for the case that we didn't fill all the B buffer with pairs
            if (amountOfPairs > 0){
                int[][] subArr = Arrays.copyOfRange(pairs, 0, amountOfPairs);
                this.sortAndWriteBlock(subArr, false, dir);
            }
            if (productIdCounter > 0){
                int[][] subArr = Arrays.copyOfRange(productIDPairs, 0, productIdCounter);
                this.sortAndWriteBlock(subArr, true, dir);
            }
            br.close();
            pairs = null;
            productIDPairs = null;
            tokens = null;
            helpfulness = null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


    private void merge(String dir, String fileName, int b, boolean isProductId) throws IOException, InterruptedException {
        int numRuns;
        if (isProductId) {
            numRuns = productIDFileIndex;
        }
        else {
            numRuns = fileIndex;
        }
        while (numRuns > 1){
            numRuns = mergeBlocks(numRuns, dir, b, fileName);
        }
    }

    private void mergeRuns(int runsToMerge, int indexStartRun, int b, String dir, String fileName) throws IOException {
        PriorityQueue<int[]> pairs = new PriorityQueue<>(runsToMerge, pairsComparator);
        int[] pair, minPair;
        int index;
        byte[] blockToRead = new byte[BLOCK_SIZE];
        ByteBuffer[] bBuffers;
        BufferedInputStream[] fileReaders = new BufferedInputStream[runsToMerge];
        BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(
                dir + File.separator + fileName + indexStartRun + ".out"), BLOCK_SIZE);
        ByteBuffer outputBuffer = ByteBuffer.allocate(BLOCK_SIZE);
        bBuffers = new ByteBuffer[runsToMerge];

        //initialize the offsets inside the runs and the buffers for the blocks of each run
        for (int i = 0; i < runsToMerge; i++){
            fileReaders[i] = new BufferedInputStream(new FileInputStream(dir+ File.separator +fileName+(b * indexStartRun + i)), BLOCK_SIZE);
            int bytesRead = fileReaders[i].read(blockToRead, 0, BLOCK_SIZE);

            bBuffers[i] = ByteBuffer.allocate(bytesRead);
            bBuffers[i].put(blockToRead, 0, bytesRead); //the buffer blocks are full
            bBuffers[i].position(0);
            pair = new int[3];
            pair[0] = bBuffers[i].getInt();
            pair[1] = bBuffers[i].getInt();
            pair[2] = i;
            pairs.add(pair);
        }

        while (runsToMerge > 0){
            minPair = pairs.poll();
            index = minPair[2];
            if (!outputBuffer.hasRemaining()) {
                writer.write(outputBuffer.array(), 0, BLOCK_SIZE);
                outputBuffer.clear();
            }
            outputBuffer.putInt(minPair[0]);
            outputBuffer.putInt(minPair[1]);

            if(!bBuffers[index].hasRemaining()){
                //if we finished a whole run
                int bytesRead = fileReaders[index].read(blockToRead, 0, BLOCK_SIZE);
                if(bytesRead != -1){

                    bBuffers[index] = ByteBuffer.allocate(bytesRead);
                    bBuffers[index].put(blockToRead, 0, bytesRead);
                    bBuffers[index].position(0);

                    pair = new int[3];
                    pair[0] = bBuffers[index].getInt();
                    pair[1] = bBuffers[index].getInt();
                    pair[2] = index;
                    pairs.add(pair);
                }
                else{

                    runsToMerge--;
                    fileReaders[index].close();
                    File oldFile = new File(dir + File.separator + fileName + (b * indexStartRun + index));
                    oldFile.delete();
                }
            }
            else {
                pair = new int[3];
                pair[0] = bBuffers[index].getInt();
                pair[1] = bBuffers[index].getInt();
                pair[2] = index;
                pairs.add(pair);
            }
        }
        if(outputBuffer.position() > 0 ){
            writer.write(outputBuffer.array(), 0, outputBuffer.position());
        }
        writer.close();
    }

    private int mergeBlocks(int totalNumOfRuns, String dir, int b, String fileName) throws IOException, InterruptedException {

        int totalNumOfRunsLeft = totalNumOfRuns;
        int runsToMerge;
        ExecutorService mergePool = Executors.newFixedThreadPool(NUM_OF_THREADS);
        int numOfNewRuns = 0;

        //runs through all the runs in the file
        while (totalNumOfRunsLeft > 0){
            runsToMerge = Math.min(totalNumOfRunsLeft, b);
            totalNumOfRunsLeft -= runsToMerge;

            int counter = numOfNewRuns;
            int runs = runsToMerge;

            mergePool.execute(()->{
                try {
                    this.mergeRuns(runs, counter, b, dir, fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            numOfNewRuns++;
        }
        mergePool.shutdown();
        mergePool.awaitTermination(100, TimeUnit.MINUTES);

        for (int i = 0; i < numOfNewRuns; i++) {
            File mergedFile = new File(dir + File.separator + fileName + i);
            File outFile = new File(dir + File.separator + fileName + i + ".out");
            outFile.renameTo(mergedFile);
        }
        return numOfNewRuns;
    }

    public static String getToken(int index, boolean isProductId) {
        if (isProductId){
            return productIdTokens.get(index);
        }
        return tokens.get(index);
    }

}
