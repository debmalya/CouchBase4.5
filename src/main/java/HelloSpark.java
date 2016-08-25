import static spark.Spark.*;

public class HelloSpark {

	public static void main(String[] args) {
		 get("/hello", (req, res) -> "Spark has ignited!!");

	}

}
