package zio

import zio.test.TestAspect.nonFlaky
import zio.test._
import zio._

object MyQueueSpec extends ZIOBaseSpec {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("sweet")(
      test("test 1") {
        val expected = Chunk.fromIterable(0 until 100)
        for {
          queue  <- Queue.bounded[Int](16)
          _      <- queue.offerAll(expected).fork
          actual <- queue.take.replicateZIO(100).map(_.to(Chunk))
        } yield assertTrue(actual == expected).label(s"Order not preserved: $actual")
      },
      test("test 2") {
        val totalSize   = 1000
        val parallelism = 5
        val queueSize   = 2

        for {
          zioQ   <- Queue.bounded[Int](queueSize)
          offers <- ZIO.forkAll(List.fill(parallelism)(zioQ.offer(0).repeatN(totalSize / parallelism).unit))
          takes  <- ZIO.forkAll(List.fill(parallelism)(zioQ.take.repeatN(totalSize / parallelism).unit))
          _      <- offers.join
          _      <- takes.join
        } yield assertCompletes
      }
    ) @@ nonFlaky(1024)

}
