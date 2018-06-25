package webdata;


import java.io.IOException;
import java.util.Enumeration;
import java.util.Vector;

public class IndexReader {


    private Dictionary dictionary;
    private Dictionary dictionaryProduct;
    private ReviewsCollection reviews;
    private String dir;

    /**
     * Creates an IndexReader which will read from the given directory
     */
    public IndexReader(String dir) {
        this.dir = dir;
        try{
            this.dictionary = Dictionary.readObject("dictionaryTable", "dictionaryString", Consts.DICTIONARY_K, dir);
            this.dictionaryProduct = Dictionary.readObject("dictionaryProductTable", "dictionaryProductString",
                    Consts.DICT_PRODUCT_ID_K, dir);
            this.reviews = ReviewsCollection.readObject("reviewsObj", dir);
        }catch (IOException e){
            System.err.println("IO Exception Error");
            System.exit(1);
        }
    }


    /**
     * Returns the product identifier for the given review
     * Returns null if there is no review with the given identifier
     */
    public String getProductId(int reviewId) {

        int posInTable;
        String currToken;
        int posProductIdInTable, positionInStr;
        if (reviewId > this.reviews.getSize() || reviewId <= 0){
            return null;
        }
        posProductIdInTable = this.reviews.getProductID(reviewId);

        int posReviewInBlock = posProductIdInTable % Consts.DICT_PRODUCT_ID_K;
        int posOfBlockHead = posProductIdInTable / Consts.DICT_PRODUCT_ID_K;

        positionInStr = this.dictionaryProduct.getTokenPtr(posOfBlockHead);
        posInTable = posOfBlockHead * Consts.DICT_PRODUCT_ID_K;
        currToken = this.dictionaryProduct.getBlockHead(posInTable);

        while(posInTable % Consts.DICT_PRODUCT_ID_K < posReviewInBlock){

            positionInStr += this.dictionaryProduct.getLength(posInTable) -
                    this.dictionaryProduct.getPrefixSize(posInTable);
            posInTable++;
            currToken = this.dictionaryProduct.getNextToken(positionInStr, currToken, posInTable);
        }
        return currToken.toUpperCase();
    }

    /**
     * Returns the score for a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewScore(int reviewId) {
        if (reviewId > this.reviews.getSize() || reviewId <= 0){
            return -1;
        }
        return this.reviews.getScore(reviewId);
    }

    /**
     * Returns the numerator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessNumerator(int reviewId) {
        if (reviewId > this.reviews.getSize() || reviewId <= 0){
            return -1;
        }
        return this.reviews.getHelpfulnessNumerator(reviewId);
    }

    /**
     * Returns the denominator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessDenominator(int reviewId) {
        if (reviewId > this.reviews.getSize() || reviewId <= 0){
            return -1;
        }
        return this.reviews.getHelpfulnessDenominator(reviewId);

    }

    /**
     * Returns the number of tokens in a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewLength(int reviewId) {
        if (reviewId > this.reviews.getSize() || reviewId <= 0){
            return -1;
        }
        return this.reviews.getLength(reviewId);
    }

    /**
     * Return the number of reviews containing a given token (i.e., word)
     * Returns 0 if there are no reviews containing this token
     */
    public int getTokenFrequency(String token) {
        int tokenIndex = this.dictionary.findTokenIndex(token.toLowerCase());
        if (tokenIndex == -1){
            return 0;
        }
        return this.dictionary.getFrequency(tokenIndex);
    }

    /**
     * Return the number of times that a given token (i.e., word) appears in
     * the reviews indexed
     * Returns 0 if there are no reviews containing this token
     */

    public int getTokenCollectionFrequency(String token){
        int collectionFreq = 0;
        int[][] postingList;
        int tokenPos = this.dictionary.findTokenIndex(token.toLowerCase());
        if (tokenPos == -1){
            return 0;
        }
        postingList = InvertedIndex.readTokenPostingList(this.dictionary.getPostingPtr(tokenPos), this.dictionary.getFrequency(tokenPos),
                Consts.INVERTED_INDEX_OBJ_FILE, this.dir);
        for (int[] postingListItem : postingList) {
            collectionFreq += postingListItem[1];
        }
        return collectionFreq;

    }

    /**
     * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
     * that id-n is the n-th review containing the given token and freq-n is the
     * number of times that the token appears in review id-n
     * Note that the integers should be sorted by id
     *
     * Returns an empty Enumeration if there are no reviews containing this token
     */
     public Enumeration<Integer> getReviewsWithToken(String token) {
         Vector<Integer> res = new Vector<>();
         int index = this.dictionary.findTokenIndex(token.toLowerCase());
         if (index == -1){
             return res.elements();
         }
         long postingPtr = this.dictionary.getPostingPtr(index);
         int[][] postingList = InvertedIndex.readTokenPostingList(postingPtr, this.dictionary.getFrequency(index),
                 Consts.INVERTED_INDEX_OBJ_FILE, this.dir);
         for (int[] postingListItem : postingList) {
             res.add(postingListItem[0]);
             res.add(postingListItem[1]);
         }
         return res.elements();
     }

     /**
     * Return the number of product reviews available in the system
     */
    public int getNumberOfReviews() {
        return this.reviews.getSize();
    }

    /**
     * Return the number of number of tokens in the system
     * (Tokens should be counted as many times as they appear)
     */
    public int getTokenSizeOfReviews() {
        return this.reviews.getNumOfTokens();
    }


    /**
     * Return the ids of the reviews for a given product identifier
     * Note that the integers returned should be sorted by id
     *
     * Returns an empty Enumeration if there are no reviews for this product
     */
    public Enumeration<Integer> getProductReviews(String productId) {
        Vector<Integer> res = new Vector<>();
        int index = this.dictionaryProduct.findTokenIndex(productId.toLowerCase());
        if (index == -1){
            return res.elements();
        }
        long postingPtr = this.dictionaryProduct.getPostingPtr(index);
        int[] postingList = InvertedIndex.readProductIdPostingList(postingPtr, this.dictionaryProduct.getFrequency(index),
                Consts.INVERTED_INDEX_PRODUCT_OBJ_FILE, this.dir);
        for (int reviewId : postingList) {
            res.add(reviewId);
        }
        return res.elements();
    }

}

