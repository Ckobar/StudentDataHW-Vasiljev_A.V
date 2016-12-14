import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Populates the collection of Students
 */

public class Students {
    private static final String VYZ_LETI = "Leti";
    private static final String VYZ_BONCH = "Bonch";
    private static final String VYZ_ITMO = "Itmo";
    private static final String VYZ_SPBGY = "Spbgy";

    private static final String HOBBY_PROGRAMMING = "programming";
    private static final String HOBBY_SPORT = "sport";
    private static final String HOBBY_GAMES = "dota";
    private static final String HOBBY_SLEEP = "time_to_sleep";

    private static final String NAME_FIELD_STUD = "name";
    private static final String VYZ_FIELD_STUD = "vyz";
    private static final String HOBBY_FIELD_STUD = "hobby";
    private static final String JOB_FIELD_STUD = "job";
    private static final String YEAR_FIELD_STUD = "year";
    private static final String COURSE_FIELD_STUD = "course";
    private static final String EVALUATION_FIELD_STUD = "evaluation";

    private static final Logger logger = Logger.getLogger(Students.class.getName());
    final Random random = new Random();
    private static final int COUNT_STUDENTS = 15;
    private static final int COUNT_COURSE = 5;
    private int year = 2016;

    /**
     * Creating the collection of Students
     */

    private void createCollection(){
        MongoClient mongoClient = new MongoClient();
        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
        MongoCollection<Document> doc = mongoDatabase.getCollection("Students");
        /*
            WARNING! DROP COLLECTION!
         */
        doc.drop();
        logger.log(Level.INFO, "Collection " + doc.getNamespace() + " was deleted");
        doc.insertMany(createDocuments());
    }

    /**
     * Creating the list of documents for the collection of Students
     * @return list List of documents for one Student
     */

    private List<Document> createDocuments(){
        List<Document> list = new LinkedList<>();
        for (int i = 0; i < COUNT_STUDENTS; i++) {
            Document document = new Document();
            document.append(NAME_FIELD_STUD, i);
            document.append(VYZ_FIELD_STUD, selectRandomVyz());
            document.append(HOBBY_FIELD_STUD, selectRandomHobby());
            document.append(JOB_FIELD_STUD, i%2==0?"yea":"nope");
            document.append(YEAR_FIELD_STUD, selectAgeStudents(i));
            document.append(COURSE_FIELD_STUD, random.nextInt(COUNT_COURSE)+1);
            document.append(EVALUATION_FIELD_STUD, createEvaluations());
            list.add(document);
        }
        return list;
    }

    /**
     * Creates a statement of estimates of per student
     * @return evaluations
     */

    private List<Integer> createEvaluations(){
        List<Integer> evaluations = new LinkedList<>();
        for (int i = 0; i < random.nextInt(20)+1; i++) {
            evaluations.add(random.nextInt(4)+2);
        }
        return evaluations;
    }

    /**
     * Selected randomly student university
     * @return university
     */

    private String selectRandomVyz(){
        int randStart = random.nextInt(4);
        if(randStart==0) return VYZ_LETI;
        if(randStart==1) return VYZ_BONCH;
        if(randStart==2) return VYZ_ITMO;
        return VYZ_SPBGY;
    }

    /**
     * Selected randomly student hobby
     * @return hobby
     */

    private String selectRandomHobby(){
        int randStart = random.nextInt(4);
        if(randStart==0) return HOBBY_PROGRAMMING;
        if(randStart==1) return HOBBY_SPORT;
        if(randStart==2) return HOBBY_GAMES;
        return HOBBY_SLEEP;
    }

    /**
     * Create randomly student age
     * @param age Random starting point for reference
     * @return age
     */

    private int selectAgeStudents(int age){
        return year-((age*10+20)/(age+1)+random.nextInt(9)+3); //random formula
    }

    public static void main(String [] argc) {
        Students randStudent = new Students();
        randStudent.createCollection();
    }
}
