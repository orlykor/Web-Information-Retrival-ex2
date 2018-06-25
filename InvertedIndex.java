package webdata;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class InvertedIndex {
    private byte[] outputBuffer = new byte[Consts.BLOCK_SIZE];
    private int bufferOffset = 0;



    public int addElementToBuffer(int elem, BufferedOutputStream writer) throws IOException {
        int numOfBytes, shift;
        byte[] newNumber = new byte[4];
        if (elem <= 63){
            numOfBytes = 1;
            shift = 0;
        }
        else if (elem > 63 && elem <= 16383){
            numOfBytes = 2;
            shift = 1 << 14;
        }
        else if (elem > 16384 && elem <= 4194303){
            numOfBytes = 3;
            shift = 2 << 22;
        }
        else {
            numOfBytes = 4;
            shift = 3 << 30;
        }

        int data = shift + elem;
        newNumber[0] = (byte) ((data & 0xFF000000) >> 24);
        newNumber[1] = (byte) ((data & 0x00FF0000) >> 16);
        newNumber[2] = (byte) ((data & 0x0000FF00) >> 8);
        newNumber[3] = (byte) (data & 0x000000FF);
        for (int i = 4 - numOfBytes; i < 4; i++) {
            if (bufferOffset == Consts.BLOCK_SIZE) {
                writer.write(outputBuffer);
                outputBuffer = new byte[Consts.BLOCK_SIZE];
                bufferOffset = 0;
            }
            outputBuffer[bufferOffset] = newNumber[i];
            bufferOffset++;
        }
        return numOfBytes;
    }

    public void writeObject(Dictionary dictionary, String fileName, String dirName, String sortedFile, boolean isProductId, int b){

        int prevReviewId = 0, nextReviewId, currReviewId = 0;
        int nextTermId, freqTokenInReview = 0, currTermId = -1;
        int bytesRead;
        long offset = 0, postingPtr = 0;
        String currToken;
        int lengthOfPostingList = 0;
        byte[] inputBuffer = new byte[b * Consts.BLOCK_SIZE];
        ByteBuffer bufferOfPairs;
        try {
            BufferedInputStream reader = new BufferedInputStream(new FileInputStream(dirName + File.separator + sortedFile+"0"));
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(dirName + File.separator + fileName));

            //block to read from sorted file
            while ((bytesRead = reader.read(inputBuffer, 0, inputBuffer.length)) != -1) {
                bufferOfPairs = ByteBuffer.wrap(inputBuffer, 0, bytesRead);

                //run thruogh all pairs in block
                while(bufferOfPairs.hasRemaining()){
                    nextTermId =  bufferOfPairs.getInt();
                    nextReviewId = bufferOfPairs.getInt();

                    // if we have a new term id
                    if(nextTermId != currTermId){
                        if (currTermId != -1){
                            offset += addElementToBuffer(currReviewId - prevReviewId, writer);
                            if (!isProductId){
                                offset += addElementToBuffer(freqTokenInReview, writer);
                            }
                            lengthOfPostingList++;
                            currToken = IndexWriter.getToken(currTermId, isProductId);
                            dictionary.addDictTableRow(currToken, lengthOfPostingList, postingPtr, currTermId, isProductId);
                            postingPtr = offset;
                        }
                        currTermId = nextTermId;
                        lengthOfPostingList = 0;
                        prevReviewId = 0;
                        freqTokenInReview = 1;
                    }
                    //the same term id
                    else{
                        //if its different review
                        if (nextReviewId != currReviewId){
                            offset += addElementToBuffer(currReviewId - prevReviewId, writer);
                            if (!isProductId){
                                offset += addElementToBuffer(freqTokenInReview, writer);
                            }

                            lengthOfPostingList++;
                            freqTokenInReview = 1;
                            prevReviewId = currReviewId;
                        }
                        else{
                            freqTokenInReview++;
                        }
                    }
                    currReviewId = nextReviewId;
                }
            }
            if(currTermId != -1){
                offset += addElementToBuffer(currReviewId - prevReviewId, writer);
                if (!isProductId){
                    offset += addElementToBuffer(freqTokenInReview, writer);
                }
                if (bufferOffset > 0){
                    writer.write(outputBuffer);
                }
                lengthOfPostingList++;
                currToken = IndexWriter.getToken(currTermId, isProductId);
                dictionary.addDictTableRow(currToken, lengthOfPostingList, postingPtr, currTermId, isProductId);
            }
            writer.close();
            reader.close();
            inputBuffer = null;

            outputBuffer = null;
            bufferOfPairs = null;
            File f = new File(dirName + File.separator + sortedFile + "0");
            f.delete();
        }catch (IOException e){
            System.err.println("IO Exception error");
            System.exit(1);
        }
    }


    private static int getIntFromByteArray(int numOfBytes, byte[] gapReviewIdBuff){
        int numFromPostingList;
        ByteBuffer wrapped = ByteBuffer.wrap(gapReviewIdBuff);
        switch (numOfBytes) {
            case 1:
                numFromPostingList = wrapped.getInt();
                break;
            case 2:
                numFromPostingList = wrapped.getInt() - (1 << 14);
                break;
            case 3:
                numFromPostingList = wrapped.getInt() - (2 << 22);
                break;
            default:
                numFromPostingList = wrapped.getInt() - (3 << 30);
        }
        wrapped = null;
        return numFromPostingList;
    }

    public static int[] readProductIdPostingList(long offset, int lengthToRead, String fileName, String dir){
        byte[] buffer = new byte[4*lengthToRead], gapReviewIdBuff;
        int numOfBytes, reviewId = 0, numFromPostingList;
        int[] postingList = new int[lengthToRead];

        try {
            RandomAccessFile inputFile = new RandomAccessFile(dir + File.separator + fileName, "r");
            inputFile.seek(offset);
            inputFile.read(buffer);
            int bufferPtr = 0;
            for (int i = 0; i < lengthToRead; i++){
                gapReviewIdBuff = new byte[4];
                numOfBytes = ((buffer[bufferPtr] >> 6) & 3) + 1;
                System.arraycopy(buffer, bufferPtr, gapReviewIdBuff, 4 - numOfBytes, numOfBytes);

                bufferPtr += numOfBytes;
                numFromPostingList = getIntFromByteArray(numOfBytes, gapReviewIdBuff);
                reviewId += numFromPostingList;
                postingList[i] = reviewId;
            }
            inputFile.close();
        }catch (IOException e){
            System.err.println("IO Exception error");
            System.exit(1);
        }
        buffer = null;
        gapReviewIdBuff = null;
        return postingList;
    }

    public static int[][] readTokenPostingList(long offset, int lengthToRead, String fileName, String dir){
        byte[] buffer = new byte[8*lengthToRead], gapReviewIdBuff;
        int numOfBytes, reviewId = 0, numFromPostingList;
        boolean isInReviewId = true;
        int[][] postingList = new int[lengthToRead][2];

        try {
            RandomAccessFile inputFile = new RandomAccessFile(dir + File.separator + fileName, "r");
            inputFile.seek(offset);
            inputFile.read(buffer);
            int bufferPtr = 0;
            for (int i = 0; i < lengthToRead * 2; i++){
                gapReviewIdBuff = new byte[4];
                numOfBytes = ((buffer[bufferPtr] >> 6) & 3) + 1;
                System.arraycopy(buffer, bufferPtr, gapReviewIdBuff, 4 - numOfBytes, numOfBytes);
                bufferPtr += numOfBytes;
                numFromPostingList = getIntFromByteArray(numOfBytes, gapReviewIdBuff);
                if (isInReviewId) {
                    reviewId += numFromPostingList;
                    isInReviewId = false;
                } else {
                    postingList[i/2][0] = reviewId;
                    postingList[i/2][1] = numFromPostingList;
                    isInReviewId = true;
                }
            }
            inputFile.close();
        }catch (IOException e){
            System.err.println("IO Exception error");
            System.exit(1);
        }
        buffer = null;
        gapReviewIdBuff = null;
        return postingList;
    }

}
