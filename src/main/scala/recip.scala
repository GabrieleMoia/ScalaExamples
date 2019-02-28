import scala.beans.BeanProperty
import scala.runtime.ScalaRunTime

class recip (
  /*00*/ @BeanProperty val id: Int,
  /*01*/ @BeanProperty val nome: String,
  /*02*/ @BeanProperty val quantità: Int,
  /*03*/ @BeanProperty val idChef: Int
  ) extends Product with Serializable {
    def copy(
              id: Int = this.id,
              nome: String = this.nome,
              quantità: Int = this.quantità,
              idChef: Int = this.idChef) = new recip(id, nome, quantità, idChef)

    override def productArity: Int = 4

    override def productElement(n: Int): Any = n match {
      case 0 => this.id
      case 1 => this.nome
      case 2 => this.quantità
      case 3 => this.idChef
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[chef]


    override def equals(that: Any) = ScalaRunTime._equals(this, that)

    override def hashCode() = ScalaRunTime._hashCode(this)

    override def toString = ScalaRunTime._toString(this)

  }

  object recip {
    def apply(
               id: Int,
               nome: String,
               quantità: Int,
               età: Int
             ) = new recip(id, nome, quantità, età)
}
