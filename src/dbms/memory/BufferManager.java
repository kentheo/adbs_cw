package dbms.memory;

import java.util.Arrays;

/**
 * Created by s1317642 on 12/03/18.
 */
public class BufferManager extends AbstractBufferManager{

    public BufferManager(Policy replPolicy, int numFrames, int pageSize, DiskSpaceManager spaceMan) {
        super(replPolicy, numFrames, pageSize, spaceMan);
    }

    @Override
    public int fetch(int pageAddr) throws FullBufferException {
        switch (replPolicy){
            case LRU: return lruFetch(pageAddr);
            case FIFO: return fifoFetch(pageAddr);
            case LIFO: return lifoFetch(pageAddr);
            case MRU: return mruFetch(pageAddr);
        }
        throw new FullBufferException();
    }

    private int mruFetch(int pageAddr) throws FullBufferException {
        // Check if the page already exists
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.pageAddress == pageAddr && !fi.isEmptyFrame()) {
                fi.pinCount++;
                mruLifoProcedure(fi);
                return fi.frameAddress;
            }
        }

        // Check if there is an empty frame, read from disk and write to buffer pool
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.isEmptyFrame()) {
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                mruLifoProcedure(fi);
                return fi.frameAddress;
            }
        }

        // Last option: Check if there is pinCount 0, read from disk and write to buffer pool
        for (FrameInfo fi: this.bookkeeping) {
            if (fi.pinCount == 0) {
                if (fi.dirty) {
                    byte[] pageToWrite = readFromBufferPool(fi.frameAddress);
                    this.spaceManager.write(fi.pageAddress, pageToWrite);
                }
                fi.dirty = false;
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                mruLifoProcedure(fi);
                return fi.frameAddress;
            }
        }
        throw new FullBufferException();
    }

    private int lifoFetch(int pageAddr) throws FullBufferException {
        // Check if the page already exists
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.pageAddress == pageAddr && !fi.isEmptyFrame()) {
                fi.pinCount++;
                return fi.frameAddress;
            }
        }

        // Check if there is an empty frame, read from disk and write to buffer pool
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.isEmptyFrame()) {
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                mruLifoProcedure(fi);
                return fi.frameAddress;
            }
        }

        // Last option: Check if there is pinCount 0, read from disk and write to buffer pool
        for (FrameInfo fi: this.bookkeeping) {
            if (fi.pinCount == 0) {
                if (fi.dirty) {
                    byte[] pageToWrite = readFromBufferPool(fi.frameAddress);
                    this.spaceManager.write(fi.pageAddress, pageToWrite);
                }
                fi.dirty = false;
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                mruLifoProcedure(fi);
                return fi.frameAddress;
            }
        }
        throw new FullBufferException();
    }

    private int fifoFetch(int pageAddr) throws FullBufferException {
        // Check if the page already exists
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.pageAddress == pageAddr && !fi.isEmptyFrame()) {
                fi.pinCount++;
                return fi.frameAddress;
            }
        }

        // Check if there is an empty frame, read from disk and write to buffer pool
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.isEmptyFrame()) {
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                lruFifoProcedure(fi);
                return fi.frameAddress;
            }
        }

        // Last option: Check if there is pinCount 0, read from disk and write to buffer pool
        for (FrameInfo fi: this.bookkeeping) {
            if (fi.pinCount == 0) {
                if (fi.dirty) {
                    byte[] pageToWrite = readFromBufferPool(fi.frameAddress);
                    this.spaceManager.write(fi.pageAddress, pageToWrite);
                }
                fi.dirty = false;
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                lruFifoProcedure(fi);
                return fi.frameAddress;
            }
        }
        throw new FullBufferException();
    }

    private int lruFetch(int pageAddr) throws FullBufferException {
        // Check if the page already exists
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.pageAddress == pageAddr && !fi.isEmptyFrame()) {
                fi.pinCount++;
                lruFifoProcedure(fi);
                return fi.frameAddress;
            }
        }

        // Check if there is an empty frame, read from disk and write to buffer pool
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.isEmptyFrame()) {
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                lruFifoProcedure(fi);
                return fi.frameAddress;
            }
        }

        // Last option: Check if there is pinCount 0, read from disk and write to buffer pool
        for (FrameInfo fi: this.bookkeeping) {
            if (fi.pinCount == 0) {
                if (fi.dirty) {
                    byte[] pageToWrite = readFromBufferPool(fi.frameAddress);
                    this.spaceManager.write(fi.pageAddress, pageToWrite);
                }
                fi.dirty = false;
                fi.pageAddress = pageAddr;
                fi.pinCount++;
                writeToBufferPool(fi);
                lruFifoProcedure(fi);
                return fi.frameAddress;
            }
        }
        throw new FullBufferException();
    }

    @Override
    public void release(int frameAddr, boolean modified) {
        for (FrameInfo fi : this.bookkeeping) {
            if (fi.frameAddress == frameAddr) {
                if (modified) fi.dirty = true;
                fi.pinCount--;
            }
        }
    }

    private void lruFifoProcedure(FrameInfo fi) {
        // Remove it and re-add it to the bottom
        this.bookkeeping.remove(this.bookkeeping.indexOf(fi));
        this.bookkeeping.add(this.bookkeeping.size(), fi);
    }

    private void mruLifoProcedure(FrameInfo fi) {
//         Remove it and re-add it to the bottom
        this.bookkeeping.remove(this.bookkeeping.indexOf(fi));
        this.bookkeeping.add(0, fi);
    }

    public void writeToBufferPool(FrameInfo fi) {
        byte[] temp = this.spaceManager.read(fi.pageAddress, this.pageSize);
        for ( int i=0; i < temp.length; i++ ) {
            this.bufferPool[fi.frameAddress+i] = temp[i];
        }
    }

    public byte[] readFromBufferPool(int addr) {
        return Arrays.copyOfRange(this.bufferPool, addr, addr+this.pageSize);
    }
}
