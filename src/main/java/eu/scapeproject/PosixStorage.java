package eu.scapeproject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.jaxb.MetsNamespacePrefixMapper;
import eu.scapeproject.model.metadata.audiomd.AudioMDMetadata;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.metadata.fits.FitsMetadata;
import eu.scapeproject.model.metadata.mix.NisoMixMetadata;
import eu.scapeproject.model.metadata.premis.PremisProvenanceMetadata;
import eu.scapeproject.model.metadata.premis.PremisRightsMetadata;
import eu.scapeproject.model.metadata.textmd.TextMDMetadata;
import eu.scapeproject.model.metadata.videomd.VideoMDMetadata;
import eu.scapeproject.model.mets.MetsMarshaller;

public class PosixStorage {
    
    private final File xmlDirectory;
    private final File datastreamDirectory;
    
    public PosixStorage(String directory) {
        File parent=new File(directory);
        if (!parent.exists()){
            parent.mkdir();
        }
        if (!parent.canExecute() || !parent.canRead() || !parent.canWrite()){
            throw new RuntimeException("Unable to access directory " + parent.getAbsolutePath());
        }
        
        xmlDirectory = new File(parent,"foxml");
        if (!xmlDirectory.exists()){
            xmlDirectory.mkdir();
        }
        if (!xmlDirectory.canExecute() || !xmlDirectory.canRead() || !xmlDirectory.canWrite()){
            throw new RuntimeException("Unable to access directory " + xmlDirectory.getAbsolutePath());
        }

        datastreamDirectory = new File(parent,"datastreams");
        if (!datastreamDirectory.exists()){
            datastreamDirectory.mkdir();
        }
        if (!datastreamDirectory.canExecute() || !datastreamDirectory.canRead() || !datastreamDirectory.canWrite()){
            throw new RuntimeException("Unable to access directory " + datastreamDirectory.getAbsolutePath());
        }
    }
    
    public void purge() throws Exception{
    	FileUtils.deleteDirectory(xmlDirectory);
    	FileUtils.deleteDirectory(datastreamDirectory);
    }
    
    public void saveXML(byte[] blob,String name,boolean overwrite) throws Exception{
        File f=new File(xmlDirectory,name + ".xml");
        if (f.exists() && !overwrite){
            throw new IOException("File " + f.getAbsolutePath() + " exists already!");
        }
        OutputStream out=null;
        try{
        	out=new FileOutputStream(f);
            IOUtils.write(blob, out);
        }finally{
            IOUtils.closeQuietly(out);
        }
    }
    
    public byte[] getXML(String id) throws Exception{
        final File f=new File(xmlDirectory,id + ".xml");
        if (!f.exists()){
            throw new FileNotFoundException("Unable to open file " + f.getAbsolutePath());
        }
        return IOUtils.toByteArray(new FileInputStream(f));
    }
}
