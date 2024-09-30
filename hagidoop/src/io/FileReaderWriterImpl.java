package io;

import java.io.*;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class FileReaderWriterImpl implements FileReaderWriter {
	
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private int format; // Format du fichier (FMT_TXT ou FMT_KV)
    private int index;
    private String fname;
   

    public FileReaderWriterImpl(String fileName, int format) {
    	this.format = format;
    	this.fname = fileName;
    }

	@Override
	public KV read() {
		return readKV();
	}

	@Override
	public void write(KV record) {
		writeKV(record);		
	}

	@Override
	public void open(String mode) {
        try {
            if ("read".equalsIgnoreCase(mode)) {
            	bufferedReader = new BufferedReader(new FileReader(fname));
            } else if ("write".equalsIgnoreCase(mode)) {
                bufferedWriter = new BufferedWriter(new FileWriter(fname));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public void close() {
		try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }		
	}

	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.fname;
	}

	@Override
	public void setFname(String filename) {
		this.fname = filename;
		
	}
	
	private KV readKV() {
		KV kv = null;
		try {
			String line = bufferedReader.readLine();
			if(line != null) {
				kv = parseKV(line);
				index++;
			}
    	} catch (Exception e) {
    		 e.printStackTrace();
    	}
        return kv;
    }
	
	private void writeKV(KV kv) {
		try {
			bufferedWriter.write(formatKV(kv));
			bufferedWriter.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	private KV parseKV(String line) {        
        if (format == FMT_KV) {            
        	String[] parts = line.split(KV.SEPARATOR);
            String key = parts[0].trim();
            String value = parts[1].trim();
            return new KV(key,value);
        } else {
            return new KV(String.valueOf(this.index), line);
        }
    }

    private String formatKV(KV record) {
        if (format == FMT_KV) {
        	return record.k + KV.SEPARATOR + record.v;
        } else {
            return record.v;
        }
    }

}
