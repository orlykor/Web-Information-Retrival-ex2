package webdata;

import java.io.*;
import java.util.concurrent.locks.ReentrantLock;


public class ReviewsCollection {

    private int[] productIDs;
    private short[] helpfulnessNumerators;
    private short[] helpfulnessDenominators;
    private byte[] scores;
    private short[] lengths;
    private int numOfTokens;
    private int tableSize;
    private ReentrantLock numOfTokensLock = new ReentrantLock();


    public ReviewsCollection(int size){
        productIDs = new int[size];
        helpfulnessNumerators = new short[size];
        helpfulnessDenominators = new short[size];
        scores = new byte[size];
        lengths = new short[size];
        this.numOfTokens = 0;
        this.tableSize = size;
    }

    public void addReview(int productIDs, short helpfulnessNumerator, short helpfulnessDenominator, byte score,
                          short length, int reviewId){
        this.productIDs[reviewId] = productIDs;
        this.helpfulnessNumerators[reviewId] = helpfulnessNumerator;
        this.helpfulnessDenominators[reviewId] = helpfulnessDenominator;
        this.scores[reviewId] = score;
        this.lengths[reviewId] = length;
        numOfTokensLock.lock();
        try {
            this.numOfTokens += length;
        }finally {
            numOfTokensLock.unlock();
        }
    }

    public int getNumOfTokens() {
        return this.numOfTokens;
    }

    public int getSize(){
        return this.tableSize;
    }

    private static byte[] convertIntToByte(int data){
        byte[] newNumber = new byte[4];
        newNumber[0] = (byte) ((data & 0xFF000000) >> 24);
        newNumber[1] = (byte) ((data & 0x00FF0000) >> 16);
        newNumber[2] = (byte) ((data & 0x0000FF00) >> 8);
        newNumber[3] = (byte) (data & 0x000000FF);
        return newNumber;
    }
    
    private static byte[] convertShortToByte(short data){
        byte[] newNumber = new byte[2];
        newNumber[0] = (byte) ((data & 0x0000FF00) >> 8);
        newNumber[1] = (byte) (data & 0x000000FF);
        return newNumber;
    }

    public void writeObject(String fileName, String dirName) throws IOException{
        File f = new File(dirName + File.separator + fileName);
        if (f.exists()) {
            f.delete();
        }
        BufferedOutputStream reviewFile = new BufferedOutputStream(new FileOutputStream(dirName + File.separator + fileName));
        reviewFile.write(convertIntToByte(this.tableSize));
        for (int i = 0; i < this.tableSize; i++) {
            reviewFile.write(convertIntToByte(this.productIDs[i]));
            reviewFile.write(convertShortToByte(this.helpfulnessNumerators[i]));
            reviewFile.write(convertShortToByte(this.helpfulnessDenominators[i]));
            reviewFile.write(this.scores[i]);
            reviewFile.write(convertShortToByte(this.lengths[i]));
        }
        reviewFile.close();
    }

    public static ReviewsCollection readObject(String fileName, String dir) throws IOException {
        RandomAccessFile reviewFile = new RandomAccessFile(dir + File.separator + fileName, "r");
        ReviewsCollection reviews = new ReviewsCollection(reviewFile.readInt());

        int productID;
        short length, helpfulnessNumerator, helpfulnessDenominator;
        byte score;

        for(int i = 0; i < reviews.tableSize; i++) {
            productID = reviewFile.readInt();
            helpfulnessNumerator = reviewFile.readShort();
            helpfulnessDenominator = reviewFile.readShort();
            score = reviewFile.readByte();
            length = reviewFile.readShort();
            reviews.addReview(productID, helpfulnessNumerator, helpfulnessDenominator, score, length, i);
        }
        reviewFile.close();
        return reviews;
    }


    public int getProductID(int index) {
        return this.productIDs[index-1];
    }

    public byte getScore(int index) {
        return this.scores[index-1];
    }

    public short getHelpfulnessNumerator(int index) {
        return this.helpfulnessNumerators[index-1];
    }

    public short getHelpfulnessDenominator(int index) {
        return helpfulnessDenominators[index-1];
    }

    public short getLength(int index) {
        return lengths[index-1];
    }
}
