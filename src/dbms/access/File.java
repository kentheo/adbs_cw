package dbms.access;

import dbms.memory.AbstractBufferManager;
import dbms.memory.FullBufferException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Created by kendeas93 on 19/03/18.
 */
public class File extends AbstractFile {

    public File(AbstractBufferManager manager, int addr) {
        super(manager, addr);
    }

    @Override
    public int printPage(int addr) throws FullBufferException {
        // Fetch page from buffer pool
        int frameAddr = this.manager.fetch(addr);
        int address_count = frameAddr;
        // First 2 bytes are signed integer in 2's complement, little-endian
        byte[] diskAddrBytes = Arrays.copyOfRange(this.manager.bufferPool, address_count, address_count + 2);
        address_count += 2;
        int next_disk_addr = convertToInt(diskAddrBytes);

        // Get number of slots of records
        byte[] one_byte = Arrays.copyOfRange(this.manager.bufferPool, address_count, address_count + 1);
        address_count++;
        int number_of_records = Byte.toUnsignedInt(one_byte[0]);

        // Padding of 5 bytes. Do nothing with them
        address_count += 5;
        // Then read records
        for (int i = 0; i < number_of_records; i++) {
            // Get first field (10 bytes)
            byte[] first_field_bytes = Arrays.copyOfRange(this.manager.bufferPool, address_count, address_count + 10);
            address_count += 10;
            // Convert bytes to a string
            String first_field = "";
            for (int j = 0; j < 10; j++) {
                first_field = first_field.concat(String.valueOf((char)first_field_bytes[j]));
            }
            // Get second field (2 bytes)
            int second_field = convertToUnsignedInt(Arrays.copyOfRange(this.manager.bufferPool, address_count, address_count + 2));
            address_count += 2;
            printRecord(first_field, second_field);
        }
        // Release frame address
        this.manager.release(frameAddr, false);
        // returns the disk address of the next page in the file
        return next_disk_addr;
    }

    public int convertToUnsignedInt(byte[] array) {
        short value = ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN).getShort();
        if (value >= 0) return value;
        else return ~value ^ 0xFFFF;
    }

    public int convertToInt(byte[] array) {
        return ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN).getShort();
    }
}
