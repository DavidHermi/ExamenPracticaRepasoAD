import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.client.*;

import java.sql.*;
import java.util.List;
import java.util.Objects;

import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.persistence.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * @author nuria
 */
public class Uvas {
    public static Statement stmt;

    /**
     * Parametros de conexion
     */
    static String bd = "postgres";
    static String login = "dam2a";
    static String password = "oracle";
    static String url = "jdbc:postgresql://localhost:5432/" + bd;

    Connection connection = null;

    /**
     * Constructor de DbConnection
     */
    public Uvas() {
        try {
            //obtenemos la conexión
            connection = DriverManager.getConnection(url, login, password);

            if (connection != null) {
                System.out.println("Conexión a base de datos " + bd + " OK\n");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Permite retornar la conexión
     */
    public Connection getConnection() {
        return connection;
    }

    public void desconectar() {
        connection = null;
    }


    public void exemplov2() throws SQLException {

        // Creating a Mongo client
        MongoClient mongo = MongoClients.create("mongodb://localhost:27017");

        // Creating Credentials
        MongoCredential credential;
        credential = MongoCredential.createCredential("Nuria", "test",
                "mongo".toCharArray());
        System.out.println("Connected to the database successfully");

        // Accessing the database
        MongoDatabase database = mongo.getDatabase("mongouvascli");
        System.out.println("Credentials ::" + credential);


        //ler datos da coleccion "uvas" de mongo
        MongoCollection<Document> collection = database.getCollection("uvas");

        stmt = connection.createStatement();

        //conexión objectdb
        // (create a new database if it doesn't exist yet):
        EntityManagerFactory emf =
                Persistence.createEntityManagerFactory("/home/dam2a/IdeaProjects/ExamenAD15_3_23/traballos.odb");
        EntityManager em = emf.createEntityManager();

        String nomeuva = null;
        int acidezmin = 0;
        int acidezmax = 0;
        String tipouvao = null;
        String coda = null;
        int acidez = 0;
        int cantidade = 0;
        String dni = null;
        int numacidez = 0;
        String tipouva = null;

        FindIterable<Document> docs = collection.find();
        MongoCursor<Document> iterator = docs.iterator();

        //bucle mongo
        while (iterator.hasNext()) {
            Document doc = iterator.next();
            tipouva = doc.getString("tipouva");
            nomeuva = doc.getString("nomeuva");
            acidezmin = doc.getInteger("acidezmin");
            acidezmax = doc.getInteger("acidezmax");

            TypedQuery<Analisis> query = em.createQuery("SELECT a FROM Analisis a ", Analisis.class);
            List<Analisis> resultados = query.getResultList();

            //Bucle del objectDB
            for (Analisis resultado : resultados) {
                coda = resultado.getCoda();
                tipouvao = resultado.getTipouva();
                acidez = resultado.getAcidez();
                cantidade = resultado.getCantidade();
                dni = resultado.getDni();

                numacidez = cantidade * 15;

                if (tipouva.equals(tipouvao)) {
                    // amosar datos da coleccion uvas
                    System.out.println("tipouva: " + tipouva + ",\tnomeuva: " + nomeuva + ",\tacidezmin: " + acidezmin + ",\tacidezmax: " + acidezmax);

                    //amosar datos del objectDB
                    System.out.println("coda: " + coda + ", tipouvao: " + tipouvao + " acidez: " + acidez + " cantidade: " + cantidade + " dni: " + dni);
                    System.out.println("numacidez: " + numacidez);
                    System.out.println("-------------------------------------");

                    //Conexion a postgres para insertar
                    Statement stmt;
                    stmt = connection.createStatement();
                    String subir = "subir acidez";
                    String bajar = "bajar acidez";
                    String igual = "acidez equilibrada";

                    if (acidez < acidezmin) {
                        stmt.executeUpdate("INSERT INTO xerado (coda, nomeuva, valor) VALUES('" + coda + "','" + nomeuva + "',('" + subir + "'," + numacidez + "));");
                    } else if (acidezmax < acidez) {
                        stmt.executeUpdate("INSERT INTO xerado (coda, nomeuva, valor) VALUES('" + coda + "','" + nomeuva + "',('" + bajar + "'," + numacidez + "));");
                    } else {
                        stmt.executeUpdate("INSERT INTO xerado (coda, nomeuva, valor) VALUES('" + coda + "','" + nomeuva + "',('" + igual + "'," + numacidez + "));");
                    }

                    // Selecciona la coleccion de clientes
                    MongoCollection<Document> collection1 = database.getCollection("clientes");
                    System.out.println("Collection sampleCollection selected successfully");

                    //Actualiza varios documentos de clientes

                    Bson query2 = eq("dni", dni);
                    Bson updates = Updates.inc("numanalisis", 1);
                    try {
                        UpdateResult result = collection1.updateMany(query2, updates);
                        System.out.println("Modified document count: " + result.getModifiedCount());
                    } catch (MongoException me) {
                        System.err.println("Unable to update due to an error: " + me);
                    }
                }
            }
        }

        iterator.close();
        em.close();
        emf.close();
    }

    public static void main(String[] args) throws SQLException {
        Uvas pv = new Uvas();
        pv.exemplov2();


    }
}