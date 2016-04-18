import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._




sealed trait GossipMessage
case class Build(neighbours: ArrayBuffer[ActorRef], s:Double, w:Double) extends GossipMessage
case object SpreadRumour extends GossipMessage
case class sendPushSum(s:Double, w: Double) extends GossipMessage
case object StartGossip extends GossipMessage
case object KillNode extends GossipMessage
case object EndPushSum extends GossipMessage
case object Spread extends GossipMessage
case object Terminate extends GossipMessage
case object ResendPushSum extends GossipMessage
case class Remove(neighbor: ActorRef) extends GossipMessage
case object FailNetwork extends GossipMessage


object RunGossip extends App{
  if(args.length != 4) {
    println("Enter arguments Number of Nodes, topology, algorithm, failureInterval in Order")
    //Don't run till you get required arguments
  }
  else{
    val system = ActorSystem("GossipNetwork")
    val master = system.actorOf(Props(new GossipMaster(args(0).toInt,args(1),args(2),args(3).toInt)), name="master")
    //Start a Rumour
    master ! StartGossip
    
  }
  
}

class GossipMaster(numOfNodes: Double, topology: String, algorithm: String, failureInterval: Int ) extends Actor{
  import context._ 
  var cubicLimit =0
   var totalSlots =0
   var networkNodes = new ArrayBuffer[ActorRef]()
   var neighbors = new ArrayBuffer[ActorRef]()
   var deadNodes = new ArrayBuffer[ActorRef]()
   var aliveNodes = new ArrayBuffer[ActorRef]()
   var deadNodeCount = 0
   var finishedNodes=0
   var StartTime: Long = 0
   
   
   //Method to Initialize Actors
   def InitializeActors = {
     for( i <- 0 until numOfNodes.toInt){
          networkNodes += context.actorOf(Props(new GossipNode(topology)), name = "NetworkNodes" + i)
        }
   }
   
   
   def receive =
   {
     case StartGossip =>
       InitializeActors  // Create Actors
       println("....build topology")
       buildTopology  // Build communication network as per topology
       aliveNodes ++= networkNodes
       println("buildTopology finished")
       //Spread a rumour in Gossip channel
       StartTime = System.currentTimeMillis()  //Start time
       
       println("....start protocol")
       if (algorithm.toLowerCase() == "gossip") { 
         networkNodes(Random.nextInt(numOfNodes.toInt)) ! SpreadRumour
       }
       else if(algorithm.toLowerCase() == "push-sum"){
         //Spread a rumour using pushsum algorithm
         networkNodes(Random.nextInt(numOfNodes.toInt)) ! sendPushSum(1,0)
       }
       else{
         println("Please input gossip or pushsum")
         context.system.shutdown()
       }
       self ! FailNetwork
     
     case FailNetwork =>
       val rumourInterval = Duration.create(failureInterval, scala.concurrent.duration.MILLISECONDS)
       if (aliveNodes.length > 0){
         val num = Random.nextInt(aliveNodes.length)
         context.stop(aliveNodes(num))
         deadNodes += aliveNodes(num)
         deadNodeCount += 1
         aliveNodes -= aliveNodes(num)
         context.system.scheduler.scheduleOnce(rumourInterval, self, FailNetwork)
       }
          
       
     case KillNode =>
       //Count the number of dead nodes. Shutdown when every node is dead
       deadNodeCount += 1
       deadNodes += sender
       if(deadNodeCount == numOfNodes.toInt-1){
         //println("All nodes are terminated..Shutting down the system")
         println("Time took to converge: " + (System.currentTimeMillis() - StartTime) +" milliseconds")
         context.system.shutdown()
       }
       if(topology.toLowerCase().equals("imp3d")){
         //val localBuffer = new ArrayBuffer[ActorRef]
         if(aliveNodes.length >0 )
           aliveNodes -= sender
         else{
           aliveNodes ++= networkNodes
           aliveNodes --= deadNodes
         }
         for (i <- 0 until aliveNodes.length){
           aliveNodes(i) ! Remove(sender)
         }
         
       }
      
     case EndPushSum =>
       finishedNodes += 1
       if(algorithm.toLowerCase() == "push-sum" && finishedNodes == 1){
         println("Convergence Time: " + (System.currentTimeMillis() - StartTime)+"milliseconds")
         context.system.shutdown()
       }
   }
   
   def buildTopology =
   {
     topology.toLowerCase() match{
       case "full" =>
         //println("Entered Full")
         for( i <- 0 until networkNodes.length){
           networkNodes(i) ! Build((networkNodes - networkNodes(i)),i+1,1)
         }
         //println("full")
       
       case "3d" =>
         cubicLimit = math.ceil(math.cbrt(numOfNodes)).toInt
         //println("Entered 3D with limit " + cubicLimit)
         
         for(i <- 0 until networkNodes.length){
           //neighbors.clear
           neighbors = new ArrayBuffer[ActorRef]()
           //Add neighbors. Maximum possible neighbors are 6
           if((i - (cubicLimit*cubicLimit)) >= 0){
             neighbors += networkNodes((i - (cubicLimit*cubicLimit)))
           }
           if((i + (cubicLimit*cubicLimit)) < numOfNodes){
             neighbors += networkNodes((i + (cubicLimit*cubicLimit)))
           }
           if((i - cubicLimit >= 0) && ((i - cubicLimit)/(cubicLimit*cubicLimit) == (i/(cubicLimit*cubicLimit)))){
             neighbors += networkNodes(i - cubicLimit)
           }
           if((i + cubicLimit < numOfNodes) && ((i + cubicLimit)/(cubicLimit*cubicLimit)) == (i/(cubicLimit*cubicLimit))){
             neighbors += networkNodes(i + cubicLimit)
           }
           if((i-1) >= 0 && ((i - 1)/cubicLimit) == (i/cubicLimit)){
             neighbors += networkNodes(i-1)
           }
           if((i+1 < numOfNodes) && ((i+1)/(cubicLimit) == (i/cubicLimit))){
             neighbors += networkNodes(i+1)
           }
           
           networkNodes(i) ! Build(neighbors,i+1,1)
           
         }
         //println("Exiting 3D")
    
         
       
       case "line" =>
         neighbors = new ArrayBuffer[ActorRef]()
         //println("Entered Line")
         for(i <- 0 until networkNodes.length){
           if(i-1 >= 0){
             neighbors += networkNodes(i-1)
           }
             
           if(i+1 < networkNodes.length){
             neighbors += networkNodes(i+1)
           }
             
           networkNodes(i) ! Build(neighbors,i+1,1)
           neighbors = new ArrayBuffer[ActorRef]()
           //println("Exiting Line")
         }
         
       case "imp3d" =>
         //println("Entered Imp3D")
         cubicLimit = math.ceil(math.cbrt(numOfNodes)).toInt
         var random = new Random
         
         for(i <- 0 until networkNodes.length){
           neighbors = new ArrayBuffer[ActorRef]()
           if((i - (cubicLimit*cubicLimit)) >= 0){
             neighbors += networkNodes((i - (cubicLimit*cubicLimit)))
           }
           if((i + (cubicLimit*cubicLimit)) < networkNodes.length){
             neighbors += networkNodes((i + (cubicLimit*cubicLimit)))
           }
           if((i - cubicLimit >= 0) && ((i - cubicLimit)/(cubicLimit*cubicLimit) == (i/(cubicLimit*cubicLimit)))){
             neighbors += networkNodes(i - cubicLimit)
           }
           if((i + cubicLimit < networkNodes.length) && ((i + cubicLimit)/(cubicLimit*cubicLimit)) == (i/cubicLimit*cubicLimit)){
             neighbors += networkNodes(i + cubicLimit)
           }
           if((i-1) >= 0 && ((i - 1)/cubicLimit) == (i/cubicLimit)){
             neighbors += networkNodes(i-1)
           }
           if((i+1 < networkNodes.length) && ((i+1)/(cubicLimit) == (i/cubicLimit))){
             neighbors += networkNodes(i+1)
           }
           
           var randomNum: Int = 0
           
           
           randomNum = random.nextInt(networkNodes.length)
           while((networkNodes(randomNum).equals(networkNodes(i)) || neighbors.contains(networkNodes(randomNum))))
             randomNum = random.nextInt(networkNodes.length)
           neighbors += networkNodes(randomNum)
         
           networkNodes(i) ! Build(neighbors,i+1,1)
           neighbors = new ArrayBuffer[ActorRef]()
           
         }
       
       case _ =>
         println("Incorrect topology")
       
     }
     
   }
}

class GossipNode(topology: String) extends Actor {
  var myNeighbors = new ArrayBuffer[ActorRef]()
  var masterNode: ActorRef = null
  var receivedRumours: Int = 0
  var myS: Double = 0
  var myW: Double = 0
  var myRatio: Double = 0
  var isTerminate:Boolean = false
  var previousRatio:Double = 0
  var currentRatio:Double = 0
  var pushCounter:Int = 0
  //var rumourInterval = Duration.create(50, scala.concurrent.duration.MILLISECONDS)
  import context._
  
  def selfTerminate = {
          isTerminate = true
          masterNode ! KillNode
          if(myNeighbors.length >= 1){
            for(i <- 0 until myNeighbors.length){
              myNeighbors(i) ! Terminate
            }
          }
          context.stop(self)
  }
  
  def receive ={
    
    case Build(neighbors,s,w) =>
      myNeighbors = neighbors
      masterNode = sender
      myS = s
      myW = w
      previousRatio = myS/myW
      
      
    case SpreadRumour =>
      if(!isTerminate){
        receivedRumours += 1
        if(receivedRumours >= 10 || myNeighbors.length <= 0){
          isTerminate = true
          selfTerminate
          
        }
        else{
          self ! Spread
        }
      }
    
    case Spread =>
      
      val rumourInterval = Duration.create(1, scala.concurrent.duration.MILLISECONDS)
      if(!isTerminate && myNeighbors.length > 0 && receivedRumours < 10){
          myNeighbors(Random.nextInt(myNeighbors.length)) ! SpreadRumour
          context.system.scheduler.scheduleOnce(rumourInterval, self, Spread)
        }
      
    case Terminate =>
      if(!isTerminate && myNeighbors.length > 0){
        myNeighbors -= sender
        if(myNeighbors.length <= 0){
          selfTerminate
        }
      }
      
    case sendPushSum(s,w) =>
      if(!isTerminate){
        myS += s
        myW += w
        currentRatio = myS / myW
        getConverge
      
        previousRatio = currentRatio
        self ! ResendPushSum
      }
      
    case ResendPushSum =>
      if(!isTerminate){
        myS /= 2
        myW /= 2
        val rumourInterval = Duration.create(1, scala.concurrent.duration.MILLISECONDS)
        myNeighbors(Random.nextInt(myNeighbors.length)) ! sendPushSum(myS, myW)
        context.system.scheduler.scheduleOnce(rumourInterval, self, ResendPushSum)
      }
      
    case Remove(neighbor) =>
      if(!isTerminate && myNeighbors.length > 0){
        myNeighbors -= neighbor
        if(myNeighbors.length <= 0){
          selfTerminate
        }
      }
    
  }
  
  def getConverge = {
    if( java.lang.Math.abs(currentRatio - previousRatio) <= 0.0000000001){
        pushCounter += 1
        if(pushCounter >= 3){
          isTerminate = true
          masterNode ! EndPushSum
          selfTerminate
        }
      }
      else{
        pushCounter = 0
      }
  }
  
}
