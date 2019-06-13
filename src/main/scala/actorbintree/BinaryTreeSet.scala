/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive

import scala.collection.immutable.{HashSet, Queue}

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef =
    context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root: ActorRef = createRoot

  // optional
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case Insert(requester, id, el) =>
      log.debug(s"=>> BTS $root ! Insert($requester, $id, $el)")
      root ! Insert(requester, id, el)
    case Contains(requester, id, el) =>
      log.debug(s"=>> BTS $root ! Contains($requester, $id, $el)")
      root ! Contains(requester, id, el)
    case Remove(requester, id, el) =>
      log.debug(s"=>> BTS $root ! Remove($requester, $id, $el)")
      root ! Remove(requester, id, el)
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      log.debug(s"=>> BTS ! CopyTo($newRoot)")
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)

    case CopyFinished =>
      pendingQueue.foreach { op =>
        newRoot ! op
      }
      pendingQueue = Queue.empty
      root = newRoot
      context.unbecome()

    case GC => /* Ignore while collecting garbage */
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) =
    Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean)
    extends Actor
    with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees: Map[Position, ActorRef] = Map[Position, ActorRef]()
  var removed: Boolean = initiallyRemoved

  // optional
  def receive: Receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {
    case Insert(requester, id, el) =>
      if (elem == el && !removed) {
        log.debug(
          s"=>> BTN $requester ! OperationFinished($id) - Duplicated? ${!removed}")
        removed = false
        requester ! OperationFinished(id)
      } else if (el < elem) {
        subtrees.get(Left) match {
          case Some(node) =>
            log.debug(s"=>> BTN $node ! L-Insert($requester, $id, $el)")
            node ! Insert(requester, id, el)
          case None =>
            // Create new node
            log.debug(s"=>> BTN $requester ! L-OperationFinished($id)")
            val newNode = context.actorOf(
              BinaryTreeNode.props(el, initiallyRemoved = false)
            )
            subtrees = subtrees.updated(Left, newNode)
            requester ! OperationFinished(id)
        }
      } else {
        subtrees.get(Right) match {
          case Some(node) =>
            log.debug(s"=>> BTN $node ! R-Insert($requester,$id, $el)")
            node ! Insert(requester, id, el)
          case None =>
            // Create new node
            log.debug(s"=>> BTN $requester ! R-OperationFinished($id)")
            val newNode = context.actorOf(
              BinaryTreeNode.props(el, initiallyRemoved = false))
            subtrees = subtrees.updated(Right, newNode)
            requester ! OperationFinished(id)
        }
      }
    case Contains(requester, id, el) =>
      if (el == elem && !removed) {
        log.debug(s"=>> BTN $requester ! ContainsResult($id, ${!removed})")
        requester ! ContainsResult(id, result = !removed)
      } else if (el < elem) {
        subtrees.get(Left) match {
          case Some(node) =>
            log.debug(s"=>> BTN $node ! L-Contains($requester, $id, $el)")
            node ! Contains(requester, id, el)
          case None =>
            log.debug(s"=>> BTN $requester ! L-ContainsResult($id, false)")
            requester ! ContainsResult(id, result = false)
        }
      } else {
        subtrees.get(Right) match {
          case Some(node) =>
            log.debug(s"=>> BTN $node ! R-Contains($requester, $id, $el)")
            node ! Contains(requester, id, el)
          case None =>
            log.debug(s"=>> BTN $requester ! R-ContainsResult($id, false)")
            requester ! ContainsResult(id, result = false)
        }
      }
    case Remove(requester, id, el) =>
      if (el == elem && !removed) {
        log.debug(
          s"=>> BTN $requester ! OperationFinished($id) - Existing? ${!removed}")
        removed = true
        requester ! OperationFinished(id)
      } else if (el < elem) {
        subtrees.get(Left) match {
          case Some(node) =>
            log.debug(s"=>> BTN $node ! L-Remove($requester, $id, $el)")
            node ! Remove(requester, id, el)
          case None =>
            log.debug(s"=>> BTN $requester ! L-OperationFinished($id)")
            requester ! OperationFinished(id)
        }
      } else {
        subtrees.get(Right) match {
          case Some(node) =>
            log.debug(s"=>> BTN $node ! R-Remove($requester, $id, $el)")
            node ! Remove(requester, id, el)
          case None =>
            log.debug(s"=>> BTN $requester ! R-OperationFinished($id)")
            requester ! OperationFinished(id)
        }
      }
    case CopyTo(treeNode) =>
      var expectedChildren = new HashSet[ActorRef]

      if (!removed) {
        log.debug(s"=>> BTN $treeNode ! CT-Insert($self, -1, $elem)")
        treeNode ! Insert(self, -1, elem)
      }

      subtrees.get(Left) match {
        case Some(node) =>
          log.debug(s"=>> BTN $node ! CT-CopyTo($treeNode)")
          expectedChildren += node
          node ! CopyTo(treeNode)
        case None =>
      }

      subtrees.get(Right) match {
        case Some(node) =>
          log.debug(s"=>> BTN $node ! CT-CopyTo($treeNode)")
          expectedChildren += node
          node ! CopyTo(treeNode)
        case None =>
      }

      if (expectedChildren.isEmpty && removed) {
        log.debug(s"=>> BTN $self ! PoisonPill")
        self ! PoisonPill
      } else {
        log.debug(s"=>> BTN become(copying($expectedChildren, $removed))")
        context.become(copying(expectedChildren, removed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive =
    LoggingReceive {
      case OperationFinished(-1) =>
        if (expected.isEmpty) {
          log.debug(s"=>> BTN $self ! PoisonPill")
          self ! PoisonPill
        } else {
          log.debug(s"=>> BTN become(copying($expected, true))")
          context.become(copying(expected, insertConfirmed = true))
        }
      case CopyFinished =>
        val newExpected = expected - sender
        if (newExpected.isEmpty && insertConfirmed) {
          log.debug(s"=>> BTN $self ! PoisonPill")
          self ! PoisonPill
        } else {
          log.debug(s"=>> BTN become(copying($newExpected, $insertConfirmed))")
          context.become(copying(newExpected, insertConfirmed))
        }
    }

  override def postStop(): Unit = {
    log.debug(s"=>> BTN parent ! CopyFinished")
    context.parent ! CopyFinished
  }

}
