import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.gt;

/**
 * Populates the collection of Employees
 */

public class Employees {
    private static final String DEVOLEPERS = "developer";
    private static final String TESTERS = "tester";
    private static final String MANAGERS = "manager";
    private static final String HR_EXPERTS = "HR-expert";

    private static final String NAME_FIELD_EMPL = "name";
    private static final String PROFILE_FIELD_EMPL = "profile";
    private static final String MIND_FIELD_EMPL = "mind";
    private static final String AGE_FIELD_EMPL = "age";
    private static final String BUG_FIELD_EMPL = "bug";

    private static final Logger logger = Logger.getLogger(Students.class.getName());
    final Random random = new Random();
    private static final int COUNT_PROGR = 15;
    private int year = 2016;

    private List<String> BUG = new LinkedList<>();

    private void createListBugs(){
        BUG.add("AnaxImperator");
        BUG.add("SagaPedo");
        BUG.add("BradyporusMultituberculatus");
        BUG.add("AphodiusBimaculatus");
        BUG.add("BrachycerusSinuatus");
        BUG.add("ProtaetiaAeruginosa");
        BUG.add("ProtaetiaSpeciosa");
        BUG.add("RhaesusSerricollis");
        BUG.add("RosaliaCoelestis");
    }

    /**
     * Creating the collection of Employees
     */

    private void createCollection(){
        MongoClient mongoClient = new MongoClient();
        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
        MongoCollection<Document> doc = mongoDatabase.getCollection("Employees");
        /*
            WARNING! DROP COLLECTION!
         */
        doc.drop();
        logger.log(Level.INFO, "Collection " + doc.getNamespace() + " was deleted");
        doc.insertMany(createDocuments());
        createListBugs();
        setCleverest(doc);
        logger.log(Level.INFO, "Collection " + doc.getNamespace() + " was created");
    }

    /**
     * Creating the list of documents for the collection of Employees
     * @return list List of documents for one employee
     */

    private List<Document> createDocuments(){
        List<Document> list = new LinkedList<>();
        for (int i = 0; i < COUNT_PROGR; i++) {
            Document document = new Document();
            document.append(NAME_FIELD_EMPL, i);
            appendRandomProf(document);
            document.append(AGE_FIELD_EMPL, selectAgeEmployees(i));
            list.add(document);
        }
        return list;
    }

    /**
     * Create employee profile
     * @param document document for one employee
     */

    private void appendRandomProf(Document document){
        String randomProf = selectRandomProf();
        document.append(PROFILE_FIELD_EMPL, randomProf);
        if(randomProf.equals(TESTERS))
            document.append(MIND_FIELD_EMPL, countMindLvl());
    }

    /**
     * Selected randomly employee profile
     * @return profile
     */

    private String selectRandomProf(){
        int randStart = random.nextInt(4);
        if(randStart==0) return DEVOLEPERS;
        if(randStart==1) return TESTERS;
        if(randStart==2) return MANAGERS;
        return HR_EXPERTS;
    }

    /**
     * Create randomly employee age
     * @param age Random starting point for reference
     * @return age
     */

    private int selectAgeEmployees(int age){
        return (age*10+30)/(age+1)+random.nextInt(11)+5; //random formula
    }

    /**
     * Create randomly employee mind
     * @return mind
     */

    private int countMindLvl(){
        return random.nextInt(10)+1;
    }

    /**
     * Creates a list of bugs for the most intelligent tester
     * @param doc the collection of Employees
     */

    private void setCleverest(MongoCollection<Document> doc){
        FindIterable<Document> iterable = doc.find(gt(MIND_FIELD_EMPL, 0));
        MongoCursor<Document> cursor = iterable.iterator();
        Map<Integer, Document> mapDocMind = new TreeMap<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            mapDocMind.put(document.getInteger(MIND_FIELD_EMPL), document);
        }
        doc.updateOne((Document) mapDocMind.values().stream().toArray()[mapDocMind.size()-1], new Document("$set", new Document(BUG_FIELD_EMPL, BUG)));
        logger.log(Level.INFO, "Bugs was created. Doc: " + mapDocMind.values().stream().toArray()[mapDocMind.size()-1]);
    }

    public static void main(String [] argc) {
        Employees employees = new Employees();
        employees.createCollection();
    }
}
