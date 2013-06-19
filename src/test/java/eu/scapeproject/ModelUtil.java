package eu.scapeproject;

import java.math.BigInteger;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.purl.dc.elements._1.ElementContainer;
import org.purl.dc.elements._1.SimpleLiteral;

import eu.scapeproject.model.BitStream;
import eu.scapeproject.model.File;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.Representation;
import gov.loc.mix.v20.BasicImageInformationType;
import gov.loc.mix.v20.BasicImageInformationType.BasicImageCharacteristics;
import gov.loc.mix.v20.Mix;
import gov.loc.mix.v20.PositiveIntegerType;

public class ModelUtil {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static final ElementContainer createDCMetadata() {
        ElementContainer dc = new ElementContainer();
        SimpleLiteral id = new SimpleLiteral();
        id.getContent().add(UUID.randomUUID().toString());
        SimpleLiteral title = new SimpleLiteral();
        title.getContent().add("A test entity");
        SimpleLiteral date = new SimpleLiteral();
        date.getContent().add(dateFormat.format(new Date()));
        SimpleLiteral lang = new SimpleLiteral();
        lang.getContent().add("en");
        dc.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "identifier"), SimpleLiteral.class, id));
        dc.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "title"), SimpleLiteral.class, title));
        dc.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "date"), SimpleLiteral.class, date));
        dc.getAny().add(new JAXBElement<SimpleLiteral>(new QName("http://purl.org/dc/elements/1.1/", "language"), SimpleLiteral.class, lang));
        return dc;
    }

    public static final IntellectualEntity createEntity(List<Representation> representations) {
        IntellectualEntity.Builder ieBuilder = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(createDCMetadata());
        if (representations != null && representations.size() != 0) {
            ieBuilder.representations(representations);
        }
        return ieBuilder.build();
    }

    public static final Representation createImageRepresentation(URI uri, List<BitStream> streams) {
    	File file = new File.Builder()
                .identifier(new Identifier("image-file-" + new Date().getTime()))
                .uri(uri)
                .technical(createNisoMetadata())
                .bitStreams(streams)
                .build();
        List<File> fList = new ArrayList<File>();
        fList.add(file);
        return new Representation.Builder(new Identifier("image-representation-" + new Date().getTime()))
                .files(fList)
                .technical(createNisoMetadata())
                .build();
    }

    private static Mix createNisoMetadata() {
        Mix mix = new Mix();
        PositiveIntegerType height = new PositiveIntegerType();
        height.setValue(new BigInteger("0"));
        BasicImageCharacteristics ch = new BasicImageCharacteristics();
        ch.setImageHeight(height);
        ch.setImageWidth(height);
        BasicImageInformationType ii = new BasicImageInformationType();
        mix.setBasicImageInformation(ii);
        mix.getBasicImageInformation().setBasicImageCharacteristics(ch);
        return mix;
    }

    public static Representation createTestRepresentation(String title) {
        File file = new File.Builder()
                .identifier(new Identifier("test-file-" + new Date().getTime()))
                .uri(URI.create("http://example.com/test-file-1"))
                .technical(createNisoMetadata())
                .bitStreams(new ArrayList<BitStream>())
                .build();
        List<File> fList = new ArrayList<File>();
        fList.add(file);
        return new Representation.Builder(new Identifier(UUID.randomUUID().toString()))
                .title(title)
                .files(fList)
                .technical(createNisoMetadata())
                .build();
    }
}
