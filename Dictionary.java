package webdata;

import java.io.*;

public class Dictionary {

    private int[] frequencies;
    private long[] postingPtrs;
    private byte[] lengths;
    private byte[] prefixSizes;
    private int[] tokenPtrs;
    private String strDict;
    private int strLength = 0;
    private int k;
    private int totalBlocks;
    private String prevToken = "";
    private int numOfRow = 0;
    private int tableSize;


    public Dictionary(int k, int size) {
        this.totalBlocks = (int)Math.ceil(size/(double)k);
        this.frequencies = new int[size];
        this.postingPtrs = new long[size];
        this.lengths = new byte[size];
        this.prefixSizes = new byte[size];
        this.tokenPtrs = new int[this.totalBlocks];
        this.k = k;
        this.tableSize = size;
    }

    private static int findCommonPrefixIndex(String s1, String s2){
        int i;
        for (i = 0; i < s1.length(); i++)
        {
            if (s1.charAt(i) != s2.charAt(i))
            {
                break;
            }
        }
        return i;
    }

    /**
     * creates the dictionary string and the table with the terms info
     */
    public void addDictTableRow(String token, int frequency, long postingPtr, int indexInTokens, boolean isProductId) {
        int prefixIndex;
        int positionInBlock = this.numOfRow % this.k;
        int length = token.length();
        if(positionInBlock == 0 ){
            this.createRow(frequency, postingPtr, (byte)length, (byte)0, strLength);
            strLength += length;
        }
        else{
            prefixIndex = findCommonPrefixIndex(this.prevToken, token);
            if (positionInBlock == this.k - 1){
                this.createRow(frequency, postingPtr, (byte)0, (byte)prefixIndex, -1);
            }
            else {
                this.createRow(frequency, postingPtr, (byte)length, (byte)prefixIndex, -1);
            }
            String newToken = token.substring(prefixIndex);
            if (isProductId){
                IndexWriter.productIdTokens.set(indexInTokens, newToken);
            }
            else {
                IndexWriter.tokens.set(indexInTokens, newToken);
            }
            strLength += newToken.length();
        }
        this.prevToken = token;
    }


    private void createRow(int frequency, long postingPtr, byte length, byte prefixSize, int tokenPtr){
        this.frequencies[this.numOfRow] = frequency;
        this.postingPtrs[this.numOfRow] = postingPtr;
        this.lengths[this.numOfRow] = length;
        this.prefixSizes[this.numOfRow] = prefixSize;
        if (tokenPtr != -1){
            this.tokenPtrs[this.numOfRow /this.k] = tokenPtr;
        }
        this.numOfRow++;
    }

    public void writeObject(String tableFileName, String strDictFileName, String dirName, boolean isProductId) throws IOException {

        int positionInBlock;
        File f1 = new File(dirName + File.separator + tableFileName);
        if (f1.exists()) {
            f1.delete();
        }
        File f2 = new File(dirName + File.separator + strDictFileName);
        if (f2.exists()) {
            f2.delete();
        }

        RandomAccessFile tableFile = new RandomAccessFile(dirName + File.separator + tableFileName, "rw");
        FileWriter fw = new FileWriter(dirName + File.separator + strDictFileName);
        BufferedWriter strDictFile = new BufferedWriter(fw);
        if (isProductId){
            strDictFile.write(String.join("", IndexWriter.productIdTokens));
        }
        else {
            strDictFile.write(String.join("", IndexWriter.tokens));
        }
        strDictFile.close();
        tableFile.writeInt(this.tableSize);
        for (int i = 0; i < this.tableSize; i++) {
            positionInBlock = i % this.k;
            tableFile.writeInt(this.frequencies[i]);
            tableFile.writeLong(this.postingPtrs[i]);

            if (positionInBlock == 0){
                tableFile.write(this.lengths[i]);
                tableFile.writeInt(this.tokenPtrs[i/this.k]);

            }
            else if (positionInBlock != this.k-1){
                tableFile.write(this.lengths[i]);
                tableFile.write(this.prefixSizes[i]);
            }
            else {
                tableFile.write(this.prefixSizes[i]);
            }
        }
        tableFile.close();

    }


   public static Dictionary readObject(String TableFileName, String strDictFileName, int k, String dir)
           throws IOException {
       int frequency, tokenPtr, positionInBlock;
       long postingPtr;
       byte length, prefixSize;
       RandomAccessFile tableFile = new RandomAccessFile(dir + File.separator + TableFileName, "r");
       Dictionary dictionary = new Dictionary(k, tableFile.readInt());

       RandomAccessFile strDict = new RandomAccessFile(dir + File.separator + strDictFileName, "r");
       dictionary.setStrDict(strDict.readLine());
       strDict.close();

       for (int i = 0; i < dictionary.tableSize; i++){
           positionInBlock = i % k;
           length = 0;
           prefixSize = 0;
           tokenPtr = -1;
           frequency = tableFile.readInt();
           postingPtr = tableFile.readLong();

           if (positionInBlock < k-1){
               length = tableFile.readByte();
           }
           if (positionInBlock > 0){
               prefixSize = tableFile.readByte();
           }
           else{
               tokenPtr = tableFile.readInt();
           }

           dictionary.createRow(frequency, postingPtr, length, prefixSize, tokenPtr);
       }

       tableFile.close();
       return dictionary;

   }

    public void setStrDict(String strDict) {
        this.strDict = strDict;
    }

    private int findBlock(String token) {
        int left = 0, middle = 0;
        int right = this.totalBlocks - 1;
        String currToken;

        while (left <= right) {
            middle = (left + right) / 2;
            currToken = this.getBlockHead(middle * this.k);
            if (currToken.compareTo(token) > 0) {
                right = middle - 1;
            } else if (currToken.compareTo(token) < 0) {
                left = middle + 1;
            }
            else{
                return middle;
            }
        }
        if (right == middle - 1){
            middle--;
        }
        else{
            if (this.totalBlocks == (middle+1)){
                return middle;
            }
            currToken = this.getBlockHead((middle+1) * this.k);
            if (currToken.compareTo(token) < 0){
                middle++;
            }
        }
        return middle;
    }

    public String getBlockHead(int position){
        int tokenPtr = this.tokenPtrs[position/this.k];
        int length = this.lengths[position];
        return this.strDict.substring(tokenPtr, tokenPtr + length);
    }

    public int getTokenPtr(int index) {
        return this.tokenPtrs[index];
    }

    public byte getPrefixSize(int index) {
        return this.prefixSizes[index];
    }
    public short getLength(int index) {
        return this.lengths[index];
    }

    public int getFrequency(int index) {
        return frequencies[index];
    }

    public long getPostingPtr(int index) {
        return postingPtrs[index];
    }

    public String getNextToken(int positionNextTokenInStr, String currToken, int positionNextTokenInTable){
        String nextToken;

        if ((positionNextTokenInTable % this.k) < this.k - 1){
            nextToken = currToken.substring(0, this.prefixSizes[positionNextTokenInTable]) +
                    this.strDict.substring(positionNextTokenInStr, positionNextTokenInStr +
                            this.lengths[positionNextTokenInTable] - this.prefixSizes[positionNextTokenInTable]);
        }
        else{
            if (positionNextTokenInTable + 1 == this.tableSize){
                //last row of table, last row of block
                nextToken = currToken.substring(0, this.prefixSizes[positionNextTokenInTable]) + this.strDict.substring(
                        positionNextTokenInStr);
            }
            else{
                nextToken = currToken.substring(0, this.prefixSizes[positionNextTokenInTable]) + this.strDict.substring(
                        positionNextTokenInStr, this.tokenPtrs[(positionNextTokenInTable + 1)/this.k]);
            }
        }
        return nextToken;
    }


    //works with the algorithm
    public int findTokenIndex(String token) {
        int positionInTable, positionInStr, blockIndex;
        String currToken;
        blockIndex = this.findBlock(token);
        if (blockIndex == -1){
            return -1;
        }
        positionInTable = blockIndex * this.k;
        currToken = this.getBlockHead(positionInTable);
        positionInStr = this.tokenPtrs[blockIndex];
        if (currToken.equals(token)){
            return positionInTable;
        }
        while(positionInTable % this.k < this.k){

            if (positionInTable + 1 == this.tableSize){
                break;
            }
            positionInStr += this.lengths[positionInTable] - this.prefixSizes[positionInTable];
            positionInTable++;
            currToken = this.getNextToken(positionInStr, currToken, positionInTable);

            if (currToken.equals(token)){
                return positionInTable;
            }
        }
        return -1;

    }

}
