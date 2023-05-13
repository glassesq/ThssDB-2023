package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.storage.DiskBuffer;

public class IndexPage extends Page {


    /**
     * create an overall page
     *
     * @param transactionId transactionId who creates this page
     * @param spaceId       spaceId
     * @param pageId        pageId
     * @param temporary     if this paged is temporary
     */
    public IndexPage(long transactionId, int spaceId, int pageId, boolean temporary) {
        this.spaceId = spaceId;
        this.pageId = pageId;
        if (!temporary) {
            DiskBuffer.put(this);
            writeAll(transactionId);
        } else {
            // TODO: temporary page.
        }
    }

}
