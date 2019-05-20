package interview

object Problem0511_3 {
  def main(args: Array[String]): Unit = {

    val dogBreeds = List("Doberman", "Yorkshire Terrier Dog", "Dachshund",
    "Scottish Terrier Dog", "Great Dane", "Portuguese Water Dog")

    val filterBreeds = for {
      breed <- dogBreeds
      if breed.contains("Terrier") && !breed.startsWith("Yorkshire")
    } yield breed

    filterBreeds.foreach(ele => println(ele))
  }

}
