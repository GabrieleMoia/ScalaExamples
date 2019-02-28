import scala.beans.BeanProperty
import scala.runtime.ScalaRunTime

class chef(
            /*00*/ @BeanProperty val idChef: Int,
            /*01*/ @BeanProperty val nomeChef: String,
            /*02*/ @BeanProperty val cognome: String,
            /*03*/ @BeanProperty val età: Int
          ) extends Product with Serializable {
  def copy(
            idChef: Int = this.idChef,
            nomeChef: String = this.nomeChef,
            cognome: String = this.cognome,
            età: Int = this.età) = new chef(idChef, nomeChef, cognome, età)

  override def productArity: Int = 4

  override def productElement(n: Int): Any = n match {
    case 0 => this.idChef
    case 1 => this.nomeChef
    case 2 => this.cognome
    case 3 => this.età
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[chef]


  override def equals(that: Any) = ScalaRunTime._equals(this, that)

  override def hashCode() = ScalaRunTime._hashCode(this)

  override def toString = ScalaRunTime._toString(this)

}

object chef {
  def apply(
             idChef: Int,
             nomeChef: String,
             cognome: String,
             età: Int
           ) = new chef(idChef, nomeChef, cognome, età)
}