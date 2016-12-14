import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;

/**
 * Created by ������� on 26.10.2016.
 * Class producing operations to create a collection, in accordance with the tasks.
 * Working with the students and to employees collections.
 */

public class Mongo {
    private static final String STUDENTS_COLL = "Students";
    private static final String EMPLOYEES_COLL = "Employees";
    private static final String DB = "test";
    private static final String AGE_EMPLOYEER = "age";
    private static final String YEAR_STUDENTS = "year";
    private static final String COURSE_STUDENTS = "course";
    private static final String BUG_EMPLOYEER = "bug";
    private static final String JOB_STUDENTS_FIELD = "job";
    private static final String JOB_STUDENTS_STATUS_EYA = "yea";
    private static final String JOB_STUDENTS_STATUS_NOPE = "nope";
    private static final String STATUS_BIG_DATA = "status";
    private static final String STATUS_BIG_DATA_STUDENTS = "recruited";
    private static final String STATUS_BIG_DATA_EMPLOYEER = "mentor";
    private static final String STUDENTS_HOBBY_FIELD = "hobby";
    private static final String STUDENTS_HOBBY_PROGRAMMING = "programming";
    private static final String PROFILE_EMPLOYEER_FIELD = "profile";
    private static final String PROFILE_EMPLOYEER_MANAGER = "manager";



    private int year = 2016;
    MongoClient mongoClient = new MongoClient();
    MongoDatabase mongoDatabase = mongoClient.getDatabase(DB);
    MongoCollection<Document> docCollEmpl = mongoDatabase.getCollection(EMPLOYEES_COLL);
    MongoCollection<Document> docCollStud = mongoDatabase.getCollection(STUDENTS_COLL);

    /**
     * Task 4 - Search age the employees and year the students
     * Finds a non-zero field and transmits processing
     * Uses global collection
     */

    private void task4CompareAge(){
        FindIterable<Document> iterableEmpl = docCollEmpl.find(gt(AGE_EMPLOYEER, 0));
        FindIterable<Document> iterableStud = docCollStud.find(gt(YEAR_STUDENTS, 0));
        findAgeEmpl(iterableEmpl);
        findAgeStud(iterableStud);
    }

    /**
     * Finds max age the employees
     * @param iterableEmpl all non-zero in the collection employees of documents
     */

    private void findAgeEmpl(FindIterable<Document> iterableEmpl){
        MongoCursor<Document> cursorEmpl = iterableEmpl.iterator();
        Map<Integer, Document> mapDocEmpl = new TreeMap<>();
        while (cursorEmpl.hasNext()) {
            Document documentAgeEmpl = cursorEmpl.next();
            mapDocEmpl.put(documentAgeEmpl.getInteger(AGE_EMPLOYEER), documentAgeEmpl);
        }
        System.out.println(mapDocEmpl.values().toArray()[mapDocEmpl.size()-1]);
    }

    /**
     * Finds min age the students
     * @param iterableStud all non-zero in the collection students of documents
     */

    private void findAgeStud(FindIterable<Document> iterableStud){
        MongoCursor<Document> cursorStud = iterableStud.iterator();
        Map<Integer, Document> mapDocStud = new TreeMap<>();
        while (cursorStud.hasNext()) {
            Document documentAgeStud = cursorStud.next();
            mapDocStud.put(documentAgeStud.getInteger(YEAR_STUDENTS), documentAgeStud);
        }
        System.out.println(mapDocStud.values().toArray()[mapDocStud.size()-1]);
    }

    /**
     * Task 5 - update age of all employees and the training course for all students
     * @param newYear year which want to change
     */

    private void task5UpdateNewYear(int newYear){
        if(year!=newYear){
            updateAgeEmpl(newYear-year);
            updateCourseStud(newYear-year);
        }
        year = newYear;
    }

    /**
     * Updates the age of employees
     * @param difference how many years have passed (the difference)
     */

    private void updateAgeEmpl(int difference){
        FindIterable<Document> iterable = docCollEmpl.find(gt(AGE_EMPLOYEER, 0));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document documentNewAgeEmpl = cursor.next();
            Document documentOldAgeEmpl = new Document(documentNewAgeEmpl);
            documentNewAgeEmpl.replace(AGE_EMPLOYEER,documentNewAgeEmpl.getInteger(AGE_EMPLOYEER)+difference);
            docCollEmpl.updateOne(documentOldAgeEmpl, new Document("$set", new Document(documentNewAgeEmpl)));
        }
    }

    /**
     * Updates the course of students
     * @param difference how many years have passed (the difference)
     */

    private void updateCourseStud(int difference){
        FindIterable<Document> iterable = docCollEmpl.find(gt(COURSE_STUDENTS, 0));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document documentNewCourseStud = cursor.next();
            Document documentOldCourseStud = new Document(documentNewCourseStud);
            documentNewCourseStud.replace(COURSE_STUDENTS,documentNewCourseStud.getInteger(COURSE_STUDENTS)+difference);
            docCollEmpl.updateOne(documentOldCourseStud, new Document("$set", new Document(documentNewCourseStud)));
        }
    }

    /**
     * Task 8 - removes bugs in the tester
     */

    private void task8DelBugs(){
        FindIterable<Document> iterable = docCollEmpl.find(exists(BUG_EMPLOYEER));
        MongoCursor<Document> cursor = iterable.iterator();
        Document documentNewBugs = cursor.next();
        Document documentOldBugs = new Document(documentNewBugs);
        List lisNewtBugs =  (List) Arrays.asList(documentOldBugs.get(BUG_EMPLOYEER)).get(0);
        List listOldBugs = new LinkedList<>(lisNewtBugs);
        listOldBugs.remove(lisNewtBugs.size()-1);
        documentNewBugs.replace(BUG_EMPLOYEER, listOldBugs);
        docCollEmpl.updateOne(documentOldBugs, new Document("$set", new Document(documentNewBugs)));
    }

    /**
     * Task 9 - hires 2 students as developers of Big Data, for one as a manager over them
     */

    private void task9SelectStudAndMenedgerForBigData(){
        List<Document> listStud = selectStudThroughJob();
        List<Document> listEmpl = selectEmplForBigData();
        if(listStud.size()>=2 && !listEmpl.isEmpty()) {
            Document documentStud0 = new Document(listStud.get(0));
            Document documentStud1 = new Document(listStud.get(1));
            Document documentEmpl0 = new Document(listEmpl.get(0));
            listStud.get(0).replace(JOB_STUDENTS_FIELD, JOB_STUDENTS_STATUS_EYA);
            listStud.get(0).append(STATUS_BIG_DATA, STATUS_BIG_DATA_STUDENTS);
            listStud.get(1).replace(JOB_STUDENTS_FIELD, JOB_STUDENTS_STATUS_EYA);
            listStud.get(1).append(STATUS_BIG_DATA, STATUS_BIG_DATA_STUDENTS);
            listEmpl.get(0).append(STATUS_BIG_DATA, STATUS_BIG_DATA_EMPLOYEER);
            docCollEmpl.updateOne(documentEmpl0, new Document("$set", new Document(listEmpl.get(0))));
            docCollStud.updateOne(documentStud0, new Document("$set", new Document(listStud.get(0))));
            docCollStud.updateOne(documentStud1, new Document("$set", new Document(listStud.get(1))));
        }
    }

    /**
     * Finds unemployed students
     * @return selectStudThroughHobbyAndJob a list of all unemployed students
     */

    private List<Document> selectStudThroughJob(){
        List<Document> listStudThroughJob = new ArrayList<>();
        FindIterable<Document> iterable = docCollStud.find(exists(JOB_STUDENTS_FIELD));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document documentJobStud = cursor.next();
            if (documentJobStud.get(JOB_STUDENTS_FIELD).equals(JOB_STUDENTS_STATUS_NOPE))
                listStudThroughJob.add(documentJobStud);
        }
        return selectStudThroughHobbyAndJob(listStudThroughJob);
    }

    /**
     * Looking for students with a hobby programming
     * @param listStudThroughJob a list of all unemployed students
     * @return listStudThroughJob a list of all unemployed students
     */

    private List<Document> selectStudThroughHobbyAndJob(List<Document> listStudThroughJob){
        return listStudThroughJob.stream().filter((documentJobAndHobbyStud) ->
                (documentJobAndHobbyStud.get(STUDENTS_HOBBY_FIELD)).equals(STUDENTS_HOBBY_PROGRAMMING)).collect(Collectors.toList());
    }

    /**
     * Looking for all the managers of the developers
     * @return listEmplForBD a list of all managers
     */

    private List<Document> selectEmplForBigData(){
        List<Document> listEmplForBD = new ArrayList<>();
        FindIterable<Document> iterable = docCollEmpl.find(exists(PROFILE_EMPLOYEER_FIELD));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document documentJobStud = cursor.next();
            if (documentJobStud.get(PROFILE_EMPLOYEER_FIELD).equals(PROFILE_EMPLOYEER_MANAGER))
                listEmplForBD.add(documentJobStud);
        }
        return listEmplForBD;
    }

    private void task10CreateIndex(){
        docCollEmpl.createIndex(new BasicDBObject("name", 1));
        docCollStud.createIndex(new BasicDBObject("name", 1));
    }

    public static void main(String [] argc){
        Mongo mongo = new Mongo();
        //mongo.task4CompareAge();
        //mongo.task5UpdateNewYear(2016);
        //mongo.task8DelBugs();
        //mongo.task9SelectStudAndMenedgerForBigData();
        //mongo.task10CreateIndex();
    }
}
