package net.lingala.zip4j.tasks;

import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.headers.HeaderUtil;
import net.lingala.zip4j.headers.HeaderWriter;
import net.lingala.zip4j.io.outputstream.SplitOutputStream;
import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.*;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.progress.ProgressMonitor;
import net.lingala.zip4j.util.Zip4jUtil;
import net.lingala.zip4j.tasks.AbstractAddFileToZipTask;
import net.lingala.zip4j.tasks.AddDataInputStreamToZipTask.AddDataInputStreamToZipTaskParameters;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static net.lingala.zip4j.util.FileUtils.getFilesInDirectoryRecursive;
import static net.lingala.zip4j.util.Zip4jUtil.getCompressionMethod;

//Custom class
public class AddDataInputStreamToZipTask extends AbstractAddFileToZipTask<AddDataInputStreamToZipTaskParameters> {

    public AddDataInputStreamToZipTask(ZipModel zipModel, char[] password, HeaderWriter headerWriter, AsyncTaskParameters asyncTaskParameters) {
        super(zipModel, password, headerWriter, asyncTaskParameters);
    }

    @Override
    protected void executeTask(AddDataInputStreamToZipTaskParameters taskParameters, ProgressMonitor progressMonitor)
            throws IOException {

        verifyZipParameters(taskParameters.zipParameters);
//
//        if (!Zip4jUtil.isStringNotNullAndNotEmpty(taskParameters.zipParameters.getFileNameInZip())) {
//            throw new ZipException("fileNameInZip has to be set in zipParameters when adding stream");
//        }

//        removeFileIfExists(getZipModel(), taskParameters.zip4jConfig, taskParameters.zipParameters.getFileNameInZip(),
//                progressMonitor);


        // For streams, it is necessary to write extended local file header because of Zip standard encryption.
        // If we do not write extended local file header, zip standard encryption needs a crc upfront for key,
        // which cannot be calculated until we read the complete stream. If we use extended local file header,
        // last modified file time is used, or current system time if not available.
        taskParameters.zipParameters.setWriteExtendedLocalFileHeader(true);

        if (taskParameters.zipParameters.getCompressionMethod().equals(CompressionMethod.STORE)) {
            // Set some random value here. This will be updated again when closing entry
            taskParameters.zipParameters.setEntrySize(0);
        }

        byte[] readBuff = new byte[taskParameters.zip4jConfig.getBufferSize()];

        //No usage of the below list since we are not going to recieve any File object, Will work on this in the future
        //List<File> updatedFilesToAdd = removeFilesIfExists(filesToAdd, zipParameters, progressMonitor, zip4jConfig);

        try (SplitOutputStream splitOutputStream = new SplitOutputStream(getZipModel().getZipFile(), getZipModel().getSplitLength());
             ZipOutputStream zipOutputStream = initializeOutputStream(splitOutputStream, taskParameters.zip4jConfig)) {

            int fileCount = taskParameters.dataInputStream.readInt();
            for(int iter = 1; iter <= fileCount; iter ++) {
                verifyIfTaskIsCancelled();
                String fileName = null;
                try {
                    fileName = taskParameters.dataInputStream.readUTF();
                } catch(UTFDataFormatException utfde) {
                    throw new ZipException("Incorrect UTF format in the file name, Gotcha!");
                }
                progressMonitor.setFileName(fileName);
                ZipParameters zipParameters = new ZipParameters();
                zipParameters.setRootFolderNameInZip(taskParameters.zipParameters.getRootFolderNameInZip());
                zipParameters.setFileNameInZip(fileName);
                addFileFromStreamToZip(taskParameters.dataInputStream, zipOutputStream, zipParameters, splitOutputStream, progressMonitor, readBuff);
            }
        }
    }

    @Override
    protected long calculateTotalWork(AddDataInputStreamToZipTaskParameters taskParameters) {
        return 0;
    }

    private void removeFileIfExists(ZipModel zipModel, Zip4jConfig zip4jConfig, String fileNameInZip,
                                    ProgressMonitor progressMonitor) throws ZipException {

        FileHeader fileHeader = HeaderUtil.getFileHeader(zipModel, fileNameInZip);
        if (fileHeader  != null) {
            removeFile(fileHeader, progressMonitor, zip4jConfig);
        }
    }

    public static class AddDataInputStreamToZipTaskParameters extends AbstractZipTaskParameters {
        private final DataInputStream dataInputStream;
        private final ZipParameters zipParameters;
        private String rootFolderName = null;

        public AddDataInputStreamToZipTaskParameters(DataInputStream dataInputStream, ZipParameters zipParameters, Zip4jConfig zip4jConfig) {
            super(zip4jConfig);
            this.dataInputStream = dataInputStream;
            this.zipParameters = zipParameters;
            try {
                this.rootFolderName = dataInputStream.readUTF();
                zipParameters.setRootFolderNameInZip(rootFolderName);
                System.out.println("Read root folder name from stream: " + rootFolderName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void setDefaultFolderPath(AddDataInputStreamToZipTaskParameters dataInputStreamToZipTaskParameters) throws IOException {
        String folderName = dataInputStreamToZipTaskParameters.rootFolderName;
        dataInputStreamToZipTaskParameters.zipParameters.setDefaultFolderPath(folderName);
    }
}
