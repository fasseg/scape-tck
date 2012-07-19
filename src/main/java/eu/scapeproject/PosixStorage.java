package eu.scapeproject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

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
import eu.scapeproject.model.mets.MetsFactory;

public class PosixStorage {
    
    private final File foxmlDirectory;
    private final File datastreamDirectory;
    private final Marshaller marshaller;
    private final Unmarshaller unmarshaller;
    
    public PosixStorage(String directory) {
        File parent=new File(directory);
        if (!parent.exists()){
            parent.mkdir();
        }
        if (!parent.canExecute() || !parent.canRead() || !parent.canWrite()){
            throw new RuntimeException("Unable to access directory " + parent.getAbsolutePath());
        }
        
        foxmlDirectory = new File(parent,"foxml");
        if (!foxmlDirectory.exists()){
            foxmlDirectory.mkdir();
        }
        if (!foxmlDirectory.canExecute() || !foxmlDirectory.canRead() || !foxmlDirectory.canWrite()){
            throw new RuntimeException("Unable to access directory " + foxmlDirectory.getAbsolutePath());
        }

        datastreamDirectory = new File(parent,"datastreams");
        if (!datastreamDirectory.exists()){
            datastreamDirectory.mkdir();
        }
        if (!datastreamDirectory.canExecute() || !datastreamDirectory.canRead() || !datastreamDirectory.canWrite()){
            throw new RuntimeException("Unable to access directory " + datastreamDirectory.getAbsolutePath());
        }
        try{
            final JAXBContext ctx = JAXBContext.newInstance(
                    MetsDocument.class,
                    DCMetadata.class,
                    TextMDMetadata.class,
                    NisoMixMetadata.class,
                    PremisProvenanceMetadata.class,
                    PremisRightsMetadata.class,
                    AudioMDMetadata.class,
                    VideoMDMetadata.class,
                    FitsMetadata.class);
            marshaller = ctx.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.setProperty("com.sun.xml.bind.namespacePrefixMapper", new MetsNamespacePrefixMapper());
            unmarshaller=ctx.createUnmarshaller();
        }catch(Exception e) {
            throw new RuntimeException(e.getLocalizedMessage(),e);
        }
    }
    
    public void save(IntellectualEntity entity,boolean overwrite) throws Exception{
        File f=new File(foxmlDirectory,entity.getIdentifier().getValue());
        if (f.exists() && !overwrite){
            throw new IOException("File " + f.getAbsolutePath() + " exists already!");
        }
        OutputStream out=null;
        try{
            MetsFactory.getInstance().serialize(entity, out);
        }finally{
            IOUtils.closeQuietly(out);
        }
    }
    
    public IntellectualEntity getEntity(String id) throws Exception{
        final File f=new File(foxmlDirectory,id);
        if (!f.exists()){
            throw new FileNotFoundException("Unable to open file " + f.getAbsolutePath());
        }
        return MetsFactory.getInstance().toIntellectualEntity((MetsDocument) unmarshaller.unmarshal(f));
    }
}
