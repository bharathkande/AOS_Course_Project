package pluradj.titan.tinkerpop3.example;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.omg.CORBA.Any;
import org.omg.CORBA.Object;

import java.io.*;
import java.util.Random;
import java.security.Key;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Service implements AutoCloseable {
    private final String FilePath = "C:\\Users\\bhara\\Desktop\\GraphDbProject";
    /**
     * There typically needs to be only one Cluster instance in an application.
     */
    private Cluster cluster;

    /**
     * Use the Cluster instance to construct different Client instances (e.g. one for sessionless communication
     * and one or more sessions). A sessionless Client should be thread-safe and typically no more than one is
     * needed unless there is some need to divide connection pools across multiple Client instances. In this case
     * there is just a single sessionless Client instance used for the entire App.
     */
    private Client client;
    private Client client2;
    private Client client3;
    private Client client4;




    /**
     * Create Service as a singleton given the simplicity of App.
     */
    private static final Service INSTANCE = new Service();
    String pick ;
    Random rand=new Random();
    LinkedHashMap<Integer, String> lhmap = new LinkedHashMap<Integer, String>();
    private Service() {
        try {
            cluster = Cluster.build(new File("conf/driver-settings.yaml")).create();
            client = cluster.connect();
            client2 = cluster.connect();
            client3 = cluster.connect();
            client4 = cluster.connect();



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    ArrayList<String> nodes = new ArrayList<>();
    public static Service getInstance() {
        return INSTANCE;
    }

    public List<String> findCreatorsOfSoftware(String softwareName) throws Exception {
        // it is very important from a performance perspective to parameterize queries
        Map params = new HashMap();
        params.put("n", softwareName);

        return client.submit("g.V().hasLabel('software').has('name',n).in('created').values('name')", params)
                .all().get().stream().map(r -> r.getString()).collect(Collectors.toList());
    }

    public void workLoads0(String sourcefile, String destinationfile) throws ExecutionException, InterruptedException, IOException {

        PrintWriter p = new PrintWriter(sourcefile);
        FileReader f = new FileReader(destinationfile);
        BufferedReader b =new BufferedReader(f);
        String combine ;
        String thisLine=null;
        int k=0;
        HashMap<String,Integer> h = new HashMap<>();
        while((thisLine= b.readLine())!=null) {
            if(k>0) {
                int counter = 0;
                if (h.containsKey(thisLine)) {
                    counter = h.get(thisLine);
                    counter++;
                    h.put(thisLine, counter);
                } else{

                    h.put(thisLine, 1);}
            }
            k++;
        }

        for(Map.Entry entry: h.entrySet())
            p.println(entry.getKey()+"\t"+entry.getValue());
        p.close();
    }


    public void step0() throws ExecutionException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\Uniform_0.txt", "UTF-8");
        long counter = 1;
        // final long workload = querryResult.size() * 15;
        int i=0;
        for (Result re : querryResult) {
            nodes.add(re.getVertex().property("userId").value().toString());
        }
        pw.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(7114);
            pick = nodes.get(n);
            pw.println(pick);
            i++;
        }
        pw.close();
        try {
            workLoads0(FilePath + "\\Uniform_0_workload.txt",FilePath + "\\Uniform_0.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void step1() throws ExecutionException, InterruptedException, IOException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\Uniform_1.txt", "UTF-8");
        ArrayList<String > neighbours = new ArrayList<>();
        int i = 0;
        pw.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(7114);
            pick = nodes.get(n);
            // pw.println(pick)
            String curent_userid = pick ;
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
            ArrayList<String> neighbouring_userIds = getneighbours(curent_userid);
            pw.println(pick);

            for(String current : neighbouring_userIds)
                pw.println(current);
            ++i;
        }
        pw.close();
        workLoads0(FilePath + "\\Uniform_1_workload.txt",FilePath + "\\Uniform_1.txt");
    }

    public void step2() throws ExecutionException, InterruptedException, IOException {
        /*

        */
        PrintWriter p3 = new PrintWriter(FilePath + "\\Un_2.txt", "UTF-8");
        ArrayList<String > neighbours = new ArrayList<>();
        int i = 0;
        String pick;
        p3.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(201523);
            pick = lhmap.get(n);
            // pw.println(pick)
            String curent_userid = pick ;
            Map pMap = new HashMap();
            pMap.put("userId", curent_userid);
            List<Result> querryResult2 = client3.submit("g.V().has('userId',userId).as('neighbours','neighboursOfneighbours','neighbours^2')" +
                    ".select('neighbours','neighboursOfneighbours','neighbours^2').by(__.both().values('userId').dedup().fold())" +
                    ".by(__.both().both().values('userId').dedup().fold())" , pMap).all().get();
            LinkedHashMap ls = (LinkedHashMap<String, Object>) (querryResult2.get(0).getObject());
            ArrayList<String> neighbouring_userIds = (ArrayList<String>) ls.get("neighbours");
            ArrayList<String> neighbouring_neighbours_userIds = (ArrayList<String>) ls.get("neighboursOfneighbours");
           // ArrayList<String> neighbouring_sq_userIds = (ArrayList<String>) ls.get("neighbours^2");

            p3.println(pick);

            for(String current : neighbouring_userIds)
                p3.println(current);

            for(String current1 : neighbouring_neighbours_userIds)
                p3.println(current1);
            ++i;
        }
        p3.close();
        workLoads0(FilePath + "\\Un_3_workload.txt",FilePath + "\\Un_3.txt");
    }

    public void displaycount() throws FileNotFoundException, UnsupportedEncodingException, ExecutionException, InterruptedException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath+"\\results.txt", "UTF-8");
        long counter = 1;
        pw.println("S.No" + "\t" + "VertexId" + "\t" + "UserId" + "\t" + "WorkLoad" + "\t\t" + "NeighboursCount"
                + "\t\t" + "Neighbours_userIds" + "\t\t" + "NeighboursOfNeighboursCount" + "\t\t" + "NeighboursOfNeighbours_userIds" + "\t\t" + "neighbours^2_Count"
                + "\t\t" + "neighbours^2_userIds");
        final long workload = querryResult.size() * 15;
        for (Result singleResult : querryResult) {

            String curent_userid = singleResult.getVertex().property("userId").value().toString();
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
//            ArrayList<String> neighbouring_userIds=getneighbours(curent_userid);
//            ArrayList<String> neighbouring_neighbours_userIds=getnonDuplicate(neighbouring_userIds);
//            for(String userId:neighbouring_userIds){
//                ArrayList<String> bufferList=getneighbours(userId);
//                neighbouring_neighbours_userIds.addAll(bufferList);
//            }
            Map pMap = new HashMap();
            pMap.put("userId", curent_userid);
            List<Result> querryResult2 = client.submit("g.V().has('userId',userId).as('neighbours','neighboursOfneighbours','neighbours^2')" +
                    ".select('neighbours','neighboursOfneighbours','neighbours^2').by(__.both().values('userId').dedup().fold())" +
                    ".by(__.both().both().values('userId').dedup().fold())" +
                    ".by(__.both().both().both().values('userId').dedup().fold())", pMap).all().get();
            LinkedHashMap ls = (LinkedHashMap<String, Object>) (querryResult2.get(0).getObject());
            ArrayList<String> neighbouring_userIds = (ArrayList<String>) ls.get("neighbours");
            ArrayList<String> neighbouring_neighbours_userIds = (ArrayList<String>) ls.get("neighboursOfneighbours");
            ArrayList<String> neighbouring_sq_userIds = (ArrayList<String>) ls.get("neighbours^2");

            pw.println(counter + "\t  " + singleResult.getVertex().id() + "\t\t  " + singleResult.getVertex().property("userId").value().toString() + "\t  " +
                    workload / querryResult.size() + "\t\t " + neighbouring_userIds.size() + "\t\t " + neighbouring_userIds.toString() + "\t\t " + neighbouring_neighbours_userIds.size()
                    + "\t\t " + neighbouring_neighbours_userIds.toString() + "\t\t " + neighbouring_sq_userIds.size() + "\t\t" + neighbouring_sq_userIds.toString());
//            System.out.println(re.getObject().toString());/


            ++counter;
        }

        pw.close();
        System.out.println(querryResult.size());
    }

    public void step3() throws ExecutionException, InterruptedException, IOException {
        PrintWriter p3 = new PrintWriter(FilePath + "\\Uniform_3.txt", "UTF-8");
        ArrayList<String > neighbours = new ArrayList<>();
        int i = 0;
        String pick;
        p3.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(7114);
            pick = nodes.get(n);
            // pw.println(pick)
            String curent_userid = pick ;
            Map pMap = new HashMap();
            pMap.put("userId", curent_userid);
            List<Result> querryResult2 = client3.submit("g.V().has('userId',userId).as('neighbours','neighboursOfneighbours','neighbours^2')" +
                    ".select('neighbours','neighboursOfneighbours','neighbours^2').by(__.both().values('userId').dedup().fold())" +
                    ".by(__.both().both().values('userId').dedup().fold())" +
                    ".by(__.both().both().both().values('userId').dedup().fold())", pMap).all().get();
            LinkedHashMap ls = (LinkedHashMap<String, Object>) (querryResult2.get(0).getObject());
            ArrayList<String> neighbouring_userIds = (ArrayList<String>) ls.get("neighbours");
            ArrayList<String> neighbouring_neighbours_userIds = (ArrayList<String>) ls.get("neighboursOfneighbours");
            ArrayList<String> neighbouring_sq_userIds = (ArrayList<String>) ls.get("neighbours^2");

            p3.println(pick);

            for(String current : neighbouring_userIds)
                p3.println(current);

            for(String current1 : neighbouring_neighbours_userIds)
                p3.println(current1);
            ++i;
        }
        p3.close();
        workLoads0(FilePath + "\\Uniform_3_workload.txt",FilePath + "\\Uniform_3.txt");

    }


    public void oldstep1() throws ExecutionException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\degreecommon.txt", "UTF-8");
        long counter = 1;
        final long workload = querryResult.size() * 15;

        for (Result re : querryResult) {
            String curent_userid = re.getVertex().property("userId").value().toString();
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
            ArrayList<String> neighbouring_userIds = getneighbours(curent_userid);


            pw.println( re.getVertex().property("userId").value().toString()+"\t"+ neighbouring_userIds.size());
            ++counter;
        }
        pw.close();
    }


    public void degreecommon() throws ExecutionException, InterruptedException, IOException {
        FileReader fr = new FileReader(FilePath + "\\degreecommon.txt");
        BufferedReader br = new BufferedReader(fr);
        String thisLine;

        String divide[] = null;
        int count = 0;
        int k = 0;
        int x=0;
        while ((thisLine = br.readLine()) != null) {

            divide = thisLine.split("\t");
            count = Integer.parseInt(divide[1]);
            for (int i = 0; i < count; i++) {
                lhmap.put(k, divide[0]);
                k++;
            }

        }
        Set set = lhmap.entrySet();
        Iterator iterator = set.iterator();
        while (iterator.hasNext()) {
            Map.Entry me = (Map.Entry) iterator.next();
            //     System.out.print("Key is: " + me.getKey() +
            //             "& Value is: " + me.getValue() + "\n");
        }
    }
    public void degree0() throws ExecutionException, InterruptedException, IOException {

        PrintWriter pw = new PrintWriter(FilePath + "\\Degree_0.txt", "UTF-8");

        // final long workload = querryResult.size() * 15;
        int i=0;
        String pick;
        pw.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(201523);
            pick = lhmap.get(n);
            pw.println(pick);
            i++;
        }
        pw.close();
        try {
            workLoads0(FilePath + "\\Degree_0_workload.txt",FilePath + "\\Degree_0.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void degree1() throws ExecutionException, InterruptedException, IOException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\degree_1.txt", "UTF-8");
        //ArrayList<String > neighbours = new ArrayList<>();
        int i = 0;
        pw.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(201523);
            pick = lhmap.get(n);
            // pw.println(pick)
            String curent_userid = pick ;
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
            ArrayList<String> neighbouring_userIds = getneighbours(curent_userid);
            pw.println(pick);
            for(String current : neighbouring_userIds)
                pw.println(current);
            ++i;
        }
        pw.close();
        workLoads0(FilePath + "\\degree_1_workload.txt",FilePath + "\\degree_1.txt");

    }
    public void degree2() throws ExecutionException, InterruptedException, IOException {

        PrintWriter pw = new PrintWriter(FilePath + "\\degree_2.txt", "UTF-8");
        // ArrayList<String > neighbours = new ArrayList<>();
        int i = 0;
        pw.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(201523);
            pick = lhmap.get(n);
            // pw.println(pick)
            String curent_userid = pick ;
            Map pMap = new HashMap();
            pMap.put("userId", curent_userid);
            List<Result> querryResult2 = client3.submit("g.V().has('userId',userId).as('neighbours','neighboursOfneighbours','neighbours^2')" +
                    ".select('neighbours','neighboursOfneighbours','neighbours^2').by(__.both().values('userId').dedup().fold())" +
                    ".by(__.both().both().values('userId').dedup().fold())" , pMap).all().get();
            LinkedHashMap ls = (LinkedHashMap<String, Object>) (querryResult2.get(0).getObject());
            ArrayList<String> neighbouring_userIds = (ArrayList<String>) ls.get("neighbours");
            ArrayList<String> neighbouring_neighbours_userIds = (ArrayList<String>) ls.get("neighboursOfneighbours");
            pw.println(pick);

            for(String current : neighbouring_userIds)
                pw.println(current);

            for(String current1 : neighbouring_neighbours_userIds)
                pw.println(current1);
            ++i;
        }
        pw.close();
        workLoads0(FilePath + "\\degree_2_workload.txt",FilePath + "\\degree_2.txt");
    }



    public void degree3() throws ExecutionException, InterruptedException, IOException {
        PrintWriter p3 = new PrintWriter(FilePath + "\\Degree_3.txt", "UTF-8");
        ArrayList<String > neighbours = new ArrayList<>();
        int i = 0;
        String pick;
        p3.println("UserId");
        while(i<10000) {
            int n = rand.nextInt(201523);
            pick = lhmap.get(n);
            // pw.println(pick)
            String curent_userid = pick ;
            Map pMap = new HashMap();
            pMap.put("userId", curent_userid);
            List<Result> querryResult2 = client3.submit("g.V().has('userId',userId).as('neighbours','neighboursOfneighbours','neighbours^2')" +
                    ".select('neighbours','neighboursOfneighbours','neighbours^2').by(__.both().values('userId').dedup().fold())" +
                    ".by(__.both().both().values('userId').dedup().fold())" +
                    ".by(__.both().both().both().values('userId').dedup().fold())", pMap).all().get();
            LinkedHashMap ls = (LinkedHashMap<String, Object>) (querryResult2.get(0).getObject());
            ArrayList<String> neighbouring_userIds = (ArrayList<String>) ls.get("neighbours");
            ArrayList<String> neighbouring_neighbours_userIds = (ArrayList<String>) ls.get("neighboursOfneighbours");
            ArrayList<String> neighbouring_sq_userIds = (ArrayList<String>) ls.get("neighbours^2");

            p3.println(pick);

            for(String current : neighbouring_userIds)
                p3.println(current);

            for(String current1 : neighbouring_neighbours_userIds)
                p3.println(current1);
            ++i;
        }
        p3.close();
        workLoads0(FilePath + "\\Degree_3_workload.txt",FilePath + "\\Degree_3.txt");

    }

    public void step2Graph() throws Exception {
//        final long workload= 50000;
        List<Result> querryResult = client2.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath+"\\Result_Step_2(NeighboursOfNeighbours)Graph.txt", "UTF-8");
        long counter = 1;
        pw.println("S.No" + "\t" + "VertexId" + "\t" + "UserId" + "\t" + "WorkLoad" + "\t\t" + "NeighboursCount"
                + "\t\t" + "Neighbours_userIds" + "\t\t" + "NeighboursOfNeighboursCount" + "\t\t" + "NeighboursOfNeighbours_userIds");
        final long workload = querryResult.size() * 15;
        for (Result singleResult : querryResult) {

            String curent_userid = singleResult.getVertex().property("userId").value().toString();
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
//            ArrayList<String> neighbouring_userIds=getneighbours(curent_userid);
//            ArrayList<String> neighbouring_neighbours_userIds=getnonDuplicate(neighbouring_userIds);
//            for(String userId:neighbouring_userIds){
//                ArrayList<String> bufferList=getneighbours(userId);
//                neighbouring_neighbours_userIds.addAll(bufferList);
//            }
            Map pMap = new HashMap();
            pMap.put("userId", curent_userid);
            List<Result> querryResult2 = client2.submit("g.V().has('userId',userId).as('neighbours','neighboursOfneighbours').select('neighbours','neighboursOfneighbours')" +
                    ".by(__.both().values('userId').dedup().fold())" +
                    ".by(__.both().both().values('userId').dedup().fold())", pMap).all().get();
            LinkedHashMap ls = (LinkedHashMap<String, Object>) (querryResult2.get(0).getObject());
            ArrayList<String> neighbouring_userIds = (ArrayList<String>) ls.get("neighbours");
            ArrayList<String> neighbouring_neighbours_userIds = (ArrayList<String>) ls.get("neighboursOfneighbours");
            pw.println(counter + "\t  " + singleResult.getVertex().id() + "\t\t  " + singleResult.getVertex().property("userId").value().toString() + "\t  " +
                    workload / querryResult.size() + "\t\t " + neighbouring_userIds.size() + "\t\t " + neighbouring_userIds.toString() + "\t\t " + neighbouring_neighbours_userIds.size()
                    + "\t\t " + neighbouring_neighbours_userIds.toString());
//            System.out.println(re.getObject().toString());/


            ++counter;
        }

        pw.close();
        System.out.println(querryResult.size());
    }
    public void Degree_step0() throws ExecutionException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        List<Result> querryResult = client.submit("g.inject('dummy').union(g.E().count(),g.V().order().by(id))").all().get();
//        List<Result> edgequery=client.submit("g.V().as('edgeCount','degree').select('edgeCount','degree').by(g.E().count()).by(__)").all().get();
        int edgecount=querryResult.get(0).getInt();
        querryResult.remove(0);
        PrintWriter pw = new PrintWriter(FilePath + "\\Result_Deg_Step_0(distict_Vertices).txt", "UTF-8");
        long counter = 1;
        final long workload = querryResult.size() * 15;

        pw.println("S.No" + "\t" + "VertexId" + "\t" + "UserId" + "\t" +"Degree"+"\t"+ "WorkLoad");
        for (Result re : querryResult) {
            int degree=getDegree(re.getVertex().property("userId").value().toString());
            Double workLoad=((double)degree /(double) edgecount)*workload;
            pw.println(counter + "\t  " + re.getVertex().id() + "\t\t  " + re.getVertex().property("userId").value().toString() + "\t "+degree+"\t "+
                    workLoad.intValue());
            ++counter;
        }
        pw.close();

    }
    public void Degree_step1() throws ExecutionException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\Result_Deg_Step_1(neighbours).txt", "UTF-8");
        long counter = 1;
        final long workload = querryResult.size() * 15;

        pw.println("S.No" + "\t" + "VertexId" + "\t" + "UserId" + "\t" + "WorkLoad" + "\t\t" + "NeighboursCount"
                + "\t\t" + "Neighbours_userIds");

        for (Result re : querryResult) {
            String curent_userid = re.getVertex().property("userId").value().toString();
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
            HashMap rescount=getDegNeighbours(curent_userid);
            ArrayList<String> neighbouring_userIds = (ArrayList<String>)rescount.get("userIds");
            Long neighcount= (Long) rescount.get("count");
            Long degree=(Long) rescount.get("degree");

            Double worlLoad=((double) degree / (double) neighcount)*workload;

            pw.println(counter + "\t  " + re.getVertex().id() + "\t\t  " + re.getVertex().property("userId").value().toString() + "\t  " +
                    worlLoad.intValue() + "\t\t " + neighbouring_userIds.size() + "\t\t " + neighbouring_userIds.toString()
            );
            ++counter;
        }
        pw.close();
    }
    public void Degree_step2() throws ExecutionException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\Result_Deg_Step_2(neighboursOfneighbours).txt", "UTF-8");
        long counter = 1;
        final long workload = querryResult.size() * 50;

        pw.println("S.No" + "\t" + "VertexId" + "\t" + "UserId" + "\t"+"TotalWorkload" +"\t" + "WorkLoad" + "\t\t" + "NeighboursCount"
                + "\t\t" + "Neighbours_userIds"+"\t\t"+"NeighboursOfNeighboursCount"+"\t\t"+"NeighboursOfNeighbours_userIds");

        for (Result re : querryResult) {
            String curent_userid = re.getVertex().property("userId").value().toString();
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
            HashMap rescount=getDegNeighbours(curent_userid);
            ArrayList<String> neighbouring_userIds = (ArrayList<String>)rescount.get("userIds");
            Long neighcount= (Long) rescount.get("count");
            Long degree=(Long) rescount.get("degree");
            HashMap mymap=getDegNeighboursofNeighbours(curent_userid);
            ArrayList NON=(ArrayList)mymap.get("userIds");
            Long neighOfneighcount= (Long) mymap.get("count");



            Double worlLoad=((double) degree / (double) (neighcount+neighOfneighcount))*workload;

            pw.println(counter + "\t  " + re.getVertex().id() + "\t\t  " + re.getVertex().property("userId").value().toString() + "\t  " +workload+"\t"+
                    worlLoad.intValue() + "\t\t " + neighbouring_userIds.size() + "\t\t " + neighbouring_userIds.toString()+"\t\t "+neighOfneighcount
                    +"\t\t "+NON.toString());
            ++counter;
        }
        pw.close();
    }
    public void Degree_step3() throws ExecutionException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        List<Result> querryResult = client.submit("g.V().order().by(id)").all().get();
        PrintWriter pw = new PrintWriter(FilePath + "\\Result_Deg_Step_3(neighbours(deg)neighbours).txt", "UTF-8");
        long counter = 1;
        final long workload = querryResult.size() * 90;

        pw.println("S.No" + "\t" + "VertexId" + "\t" + "UserId" + "\t" +"TotalWorkLoad"+"\t"+ "WorkLoad" + "\t\t" + "NeighboursCount"
                + "\t\t" + "Neighbours_userIds"+"\t\t"+"NeighboursOfNeighboursCount"+"\t\t"+"NeighboursOfNeighbours_userIds"+"\t\t"+"Neighbours^2NeighboursCount"+"\t\t"+
                "Neighbours^2Neighbours_userIds");

        for (Result re : querryResult) {
            String curent_userid = re.getVertex().property("userId").value().toString();
//            List<Result> neighbouring_result=client.submit("g.V().has('userId',curent_userid).both()",params).all().get();
            HashMap rescount=getDegNeighbours(curent_userid);
            ArrayList<String> neighbouring_userIds = (ArrayList<String>)rescount.get("userIds");
            Long neighcount= (Long) rescount.get("count");
            Long degree=(Long) rescount.get("degree");
            HashMap NONmap=getDegNeighboursofNeighbours(curent_userid);
            ArrayList NON=(ArrayList)NONmap.get("userIds");
            Long neighOfneighcount= (Long) NONmap.get("count");
            HashMap NONmap2=getDegNeighboursofNeighbours2(curent_userid);
            ArrayList NON2=(ArrayList)NONmap.get("userIds");
            Long neighOfneighcount2= (Long) NONmap.get("count");




            Double worlLoad=((double) degree / (double) (neighcount+neighOfneighcount+neighOfneighcount2))*workload;

            pw.println(counter + "\t  " + re.getVertex().id() + "\t\t  " + re.getVertex().property("userId").value().toString() + "\t  " +workload+"\t"+
                    worlLoad.intValue() + "\t\t " + neighbouring_userIds.size() + "\t\t " + neighbouring_userIds.toString()+"\t\t "+neighOfneighcount
                    +"\t\t "+NON.toString()+"\t\t" +neighOfneighcount2+"\t\t"+NON2.toString());
            ++counter;
        }
        pw.close();
    }


    @Override
    public void close() throws Exception {
        client.close();
        client2.close();
        client3.close();
        client4.close();

        cluster.close();
    }

    public ArrayList<String> getneighbours(String userId) throws ExecutionException, InterruptedException {
        Map paramMap = new HashMap();
        paramMap.put("userId", userId);
        List<Result> qerryResult = client4.submit("g.V().has('userId',userId).both()", paramMap).all().get();

        ArrayList<String> neighbouring_userIds = new ArrayList<>();
        for (Result rs : qerryResult) {
            if (!neighbouring_userIds.contains(rs.getVertex().property("userId").value().toString()))
                neighbouring_userIds.add(rs.getVertex().property("userId").value().toString());
        }
        return neighbouring_userIds;
    }
    public HashMap<String,Object> getDegNeighbours(String userId) throws ExecutionException, InterruptedException {
        Map paramMap = new HashMap();
        paramMap.put("userId", userId);
        Long count=0l;
        List<Result> qerryResult = client.submit(" g.V().has('userId',userId).both().dedup().as('neighbour','neighbourCount').select('neighbour','neighbourCount').by(__.values('userId').fold()).by(__.both().dedup().count())",paramMap).all().get();
        List<Result> degreeResult=client.submit("g.V().has('userId',userId).both().count()",paramMap).all().get();
        Long degree=degreeResult.get(0).getLong();
        ArrayList<String> neighbouring_userIds = new ArrayList<>();
        for (Result rs : qerryResult) {
//           neighbouring_userIds.add(((ArrayList<String>)((LinkedHashMap) rs.getObject()).get("neighbour")).get(0));
            if (!neighbouring_userIds.contains(((ArrayList<String>)((LinkedHashMap) rs.getObject()).get("neighbour")).get(0))) {
                neighbouring_userIds.add(((ArrayList<String>) ((LinkedHashMap) rs.getObject()).get("neighbour")).get(0));
                count += (Long) ((LinkedHashMap) rs.getObject()).get("neighbourCount");
            }
        }
        HashMap result=new HashMap();
        result.put("userIds",neighbouring_userIds);
        result.put("count",count);
        result.put("degree",degree);

        return result;
    }
    public HashMap<String,Object> getDegNeighboursofNeighbours(String userId) throws ExecutionException, InterruptedException {
        Map paramMap = new HashMap();
        paramMap.put("userId", userId);
        Long count=0l;
        List<Result> qerryResult = client.submit("  g.V().has('userId',userId).both().dedup().both().dedup().as('neighbour','neighbourCount').select('neighbour','neighbourCount').by(__.values('userId').fold()).by(__.both().dedup().count())",paramMap).all().get();
//        List<Result> degreeResult=client.submit("g.V().has('userId',userId).both().count()",paramMap).all().get();
//        Long degree=degreeResult.get(0).getLong();
        ArrayList<String> neighbouring_userIds = new ArrayList<>();
        for (Result rs : qerryResult) {
//           neighbouring_userIds.add(((ArrayList<String>)((LinkedHashMap) rs.getObject()).get("neighbour")).get(0));
            if (!neighbouring_userIds.contains(((ArrayList<String>)((LinkedHashMap) rs.getObject()).get("neighbour")).get(0))) {
                neighbouring_userIds.add(((ArrayList<String>) ((LinkedHashMap) rs.getObject()).get("neighbour")).get(0));
                count += (Long) ((LinkedHashMap) rs.getObject()).get("neighbourCount");
            }
        }
        HashMap result=new HashMap();
        result.put("userIds",neighbouring_userIds);
        result.put("count",count);
//        result.put("degree",degree);

        return result;
    }
    public HashMap<String,Object> getDegNeighboursofNeighbours2(String userId) throws ExecutionException, InterruptedException {
        Map paramMap = new HashMap();
        paramMap.put("userId", userId);
        Long count=0l;
        List<Result> qerryResult = client.submit("  g.V().has('userId',userId).both().dedup().both().dedup().both().dedup().as('neighbour','neighbourCount').select('neighbour','neighbourCount').by(__.values('userId').fold()).by(__.both().dedup().count())",paramMap).all().get();
//        List<Result> degreeResult=client.submit("g.V().has('userId',userId).both().count()",paramMap).all().get();
//        Long degree=degreeResult.get(0).getLong();
        ArrayList<String> neighbouring_userIds = new ArrayList<>();
        for (Result rs : qerryResult) {
//           neighbouring_userIds.add(((ArrayList<String>)((LinkedHashMap) rs.getObject()).get("neighbour")).get(0));
            if (!neighbouring_userIds.contains(((ArrayList<String>)((LinkedHashMap) rs.getObject()).get("neighbour")).get(0))) {
                neighbouring_userIds.add(((ArrayList<String>) ((LinkedHashMap) rs.getObject()).get("neighbour")).get(0));
                count += (Long) ((LinkedHashMap) rs.getObject()).get("neighbourCount");
            }
        }
        HashMap result=new HashMap();
        result.put("userIds",neighbouring_userIds);
        result.put("count",count);
//        result.put("degree",degree);

        return result;
    }

    public int getDegree(String usrId) throws ExecutionException, InterruptedException {
        Map pMap=new HashMap();
        pMap.put("userId",usrId);
        List<Result> querryresult=client4.submit("g.V().has('userId',userId).both().dedup().count()",pMap).all().get();
        return querryresult.get(0).getInt();
    }

    public ArrayList<String> getnonDuplicate(ArrayList<String> neighboringlist) throws ExecutionException, InterruptedException {
        ArrayList<String> finalList = new ArrayList<>();

        for (String userId : neighboringlist) {
            ArrayList<String> bufferList = getneighbours(userId);
            for (String elementFrombuffer : bufferList) {
                if (!finalList.contains(elementFrombuffer))
                    finalList.add(elementFrombuffer);
            }
        }
        return finalList;
    }
}