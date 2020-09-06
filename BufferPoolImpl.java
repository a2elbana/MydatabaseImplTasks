
package dbis.toydb.bp.impl;

import dbis.toydb.pf.*;
import dbis.toydb.util.DBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Iterator;

import java.util.List;

import dbis.toydb.bp.BufferPool;

/*
 * See "BufferPool.java" for further information about the functions and its parameters
 */
/*
 * BufferPool mit ARC als Seitenersetzungsstrategie
 * @author Ahmed Elbana
 */
public class BufferPoolImpl implements BufferPool {
	
	//add necessary data structures here, e.g.
	//an integer for maximum size,
	//a container for pages,
	//etc.

    private final HashMap<Integer, QueueNode<Page>> nodeData;

    private final QueueNode<Page> t1Head;
    private final QueueNode<Page> t2Head;
    private final QueueNode<Page> b1Head;
    private final QueueNode<Page> b2Head;
	int p;
	int t1Size;
	int t2Size;
	int b1Size;
	int b2Size;
	int maxSizeOfCache;
	List<QueueNode> buffer;

	



	/*
	 * Constructor for BufferPool.
	 * @param max: Amount of pages the pool can hold
	 */
	public BufferPoolImpl(int max) {
		super();
		//do some more initialization stuff here, e.g. setting max size
        this.maxSizeOfCache = max;
        this.nodeData = new HashMap<>();

        this.t1Head = new QueueNode<>();
        this.t2Head = new QueueNode<>();
        this.b1Head = new QueueNode<>();
        this.b2Head = new QueueNode<>();
        this.buffer = new ArrayList<>();

        
	}

	
	public void cache(int key, Page value) {
        QueueNode<Page> queueNode = nodeData.get(key);

        if (queueNode == null) {
            onMissOnT1orT2orB1orB2(key, value);
        } else if (queueNode.type == QueueTypeEnum.B1) {
            queueNode.setData(value);
            onHitOnB1(queueNode);
        } else if (queueNode.type == QueueTypeEnum.B2) {
            queueNode.setData(value);
            onHitOnB2(queueNode);
        } else {
            queueNode.setData(value);
            onHitOnT1orT2(queueNode);
        }
    }

	/*
	 * Default constructor for BufferPool.
	 * Creates a buffer pool of maximum size 100.
	 * (Nothing to do here)
	 */
	public BufferPoolImpl() {
		this(100);
	}

//  Hit in ARC( c) and DBL(2c)
  private void onHitOnT1orT2(QueueNode queueNode) {

      if (queueNode.type == QueueTypeEnum.T1) {
          t1Size--;
          t2Size++;
      }
      queueNode.remove();
      queueNode.type = QueueTypeEnum.T2;
      queueNode.addToLast(t2Head);
  }

//  miss in ARC(c), hit in DBL(2c)
  private void onHitOnB1(QueueNode queueNode) {

      p = Math.min(maxSizeOfCache, p + Math.max(b2Size / b1Size, 1));
      replace(queueNode);

      t2Size++;
      b1Size--;
      queueNode.remove();
      queueNode.type = QueueTypeEnum.T2;
      queueNode.addToLast(t2Head);
  }

//  miss in ARC(c), hit in DBL(2c)
  private void onHitOnB2(QueueNode queueNode) {

      p = Math.max(0, p - Math.max(b1Size / b2Size, 1));
      replace(queueNode);

      t2Size++;
      b2Size--;
      queueNode.remove();
      queueNode.type = QueueTypeEnum.T2;
      queueNode.addToLast(t2Head);
  }

//  miss in DBL(2c) and ARC(c)
  private void onMissOnT1orT2orB1orB2(int key, Page value) {

      QueueNode<Page> queueNode = new QueueNode<Page>();
      queueNode.type = QueueTypeEnum.T1;

      int sizeL1 = (t1Size + b1Size);
      int sizeL2 = (t2Size + b2Size);
      if (sizeL1 == maxSizeOfCache) {
          if (t1Size < maxSizeOfCache) {
              QueueNode<Page> queueNodeToBeRemoved = b1Head.next;
              removeDataFromQueue(queueNodeToBeRemoved);
              queueNodeToBeRemoved.remove();
              b1Size--;

              replace(queueNode);
          } else {
              QueueNode queueNodeToBeRemoved = t1Head.next;
              removeDataFromQueue(queueNodeToBeRemoved);
              queueNodeToBeRemoved.remove();
              t1Size--;
          }
      } else if ((sizeL1 < maxSizeOfCache) && ((sizeL1 + sizeL2) >= maxSizeOfCache)) {
          if ((sizeL1 + sizeL2) >= (2 * maxSizeOfCache)) {
              QueueNode<Page> queueNodeToBeRemoved = b2Head.next;
              removeDataFromQueue(queueNodeToBeRemoved);
              queueNodeToBeRemoved.remove();
              b2Size--;
          }
          replace(queueNode);
      }

      t1Size++;
      nodeData.put(key, queueNode);
      queueNode.addToLast(t1Head);

  }


	private void removeDataFromQueue(QueueNode queueNodeToBeRemoved) {
	// TODO Auto-generated method stub
        nodeData.remove(queueNodeToBeRemoved.key);
        Page data = (Page) queueNodeToBeRemoved.getData();
	
}


	/*
	 * Function for returning the requested page.
	 * Returns null if page does not exist, else the corresponding page is returned.
	 */
	public synchronized  Page lookupPage(PagedFile pFile, int pageNum, boolean ref) throws DBException {
		Long searchedPageNum = globalPageNum(pFile, pageNum);
		for (QueueNode QueueNode : buffer) {
			if (searchedPageNum == QueueNode.getGlobalPageNum()) {
				setBitPageToZeroIfRef(ref, QueueNode);
				return (Page) QueueNode.getData();
			}
		}
		return null;
	}
	
	private void setBitPageToZeroIfRef(boolean ref, QueueNode QueueNode) {
		if (ref && QueueNode.getBit() == 0) {
			QueueNode.setBit(1);
		}
	}



	/*
	 * A new page is added to the buffer pool.
	 * If the maximum buffer size is reached, another page has to become evicted.
	 * Note 1: Care for pages whose content has changed!
	 * Note 2: Care for pinned pages!
	 * Note 3: If no page can be evicted a DBException should be thrown
	 */
	public synchronized void addPage(PagedFile pfile, Page page) throws DBException {
		Page bufferedPage = lookupPage(pfile, page.getPageNum(), true); 
		//return if the page is already in the buffer
		if (bufferedPage != null) {
			return;
		}
		if (bufferHasFreeSpace()) {
			buffer.add(new QueueNode(b1Size, page, globalPageNum(pfile, page.getPageNum()), 1));
		} else {
			boolean pageReplaced = true;
			if (!pageReplaced) {
				throw new DBException("the page: " + page.getPageNum() + " can not be added");
			}
		}
	}


	/**
	 * @return true if the buffer has place to add a new page.
	 */
	private boolean bufferHasFreeSpace() {
		return buffer.size() < maxSizeOfCache;
	}
	
	
//  Replace function
  private synchronized void replace(QueueNode candidate) {

      if ((t1Size >= 1) && (((candidate.type == QueueTypeEnum.B2) && (t1Size == p)) || (t1Size > p))) {
          QueueNode<Page> queueNodeToBeRemoved = t1Head.next;
          queueNodeToBeRemoved.remove();
          queueNodeToBeRemoved.type = QueueTypeEnum.B1;
          queueNodeToBeRemoved.addToLast(b1Head);
          t1Size--;
          b1Size++;
      } else {
          QueueNode<Page> queueNodeToBeRemoved = t2Head.next;
          queueNodeToBeRemoved.remove();
          queueNodeToBeRemoved.type = QueueTypeEnum.B2;
          queueNodeToBeRemoved.addToLast(b2Head);
          t2Size--;
          b2Size++;
      }

  }
	
	/*
	 * A given page is removed from buffer (if it exists).
	 */
	public synchronized  void removePage(PagedFile pfile, int pageNum) {
		Iterator<QueueNode> iterator = buffer.iterator();
		while (iterator.hasNext()) {
			QueueNode queueNode = iterator.next();
			if (queueNode.getGlobalPageNum() == globalPageNum(pfile, pageNum)) {
				iterator.remove();
				return;
			}
		}

	}


	/*
	 * All pages of the given PagedFile are written to disk.
	 * The function "pf.forcePage(...)" from PagedFile can be used.
	 */
	public synchronized  void forceAllPages(PagedFile pf) throws DBException {
		for (QueueNode queueNode : buffer) {
			if (queueNode.getGlobalPageNum() == globalPageNum(pf, ((Page) queueNode.getData()).getPageNum())) {
				try {
					pf.forcePage((Page) queueNode.getData());
				} catch (DBException e) {
					System.err.println(e.getMessage());
				}
			}
		}

	}


	/*
	 * All pages of the given PagedFile are removed from the buffer pool.
	 */
	public void removeAllPages(PagedFile pf) throws DBException {
		Iterator<QueueNode> iterator = buffer.iterator();
		
		while (iterator.hasNext()) {
			QueueNode queueNode = iterator.next();
			nodeData.remove(queueNode.key);

			}
			
		}

	


	/*
	 * Changes the maximum size of this buffer pool.
	 * What happens if the size is smaller than before?
	 */
	public void resizeBuffer(int newSize) throws DBException {

		if (buffer.size() <= newSize) {
			this.maxSizeOfCache = newSize;
		} {
				throw new DBException("can not be resize: " + newSize + "old: " + this.maxSizeOfCache);
			}
		}
	


	/*
	 * Returns the maximum size of this buffer pool.
	 */
	public int getSize() {
		return buffer.size();
	}


	/*
	 * Prints all pages this buffer pool currently holds
	 */
	public void show() throws DBException {
		synchronized (buffer) {
			for (QueueNode QueueNode : buffer) {
				System.out.println(QueueNode.getData().toString());
			}
		}
	}


	/*
	 * Computes a global page identifier for a given page
	 */
	private long globalPageNum(PagedFile pfile, int pageNum) {
		return (((long)pfile.hashCode() << 32) | (long)pageNum);
	}
}
