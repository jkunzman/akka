package akka.testkit

import org.scalatest.matchers.MustMatchers
import org.scalatest.{ BeforeAndAfterEach, WordSpec }
import scala.concurrent.duration._
import com.typesafe.config.Config
import org.scalatest.exceptions.TestFailedException

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class TestTimeSpec extends AkkaSpec(Map("akka.test.timefactor" -> 2.0)) with BeforeAndAfterEach {

  "A TestKit" must {

    "correctly dilate times" in {
      1.second.dilated.toNanos must be(1000000000L * testKitSettings.TestTimeFactor)
    }

    "awaitAssert must throw correctly" in {
      awaitAssert("foo" must be("foo"))
      within(300.millis, 2.seconds) {
        intercept[TestFailedException] {
          awaitAssert("foo" must be("bar"), 500.millis, 300.millis)
        }
      }
    }

  }

}
