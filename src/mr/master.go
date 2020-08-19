package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

type Master struct {
	/**
	* mapTask，Task类数组
	* reduceTask，Task类数组
	* intermediateFile，映射数组，保存中间文件
	* nReduce，整数，记录每个map任务划分多少份
	* masterState，记录master的状态
	* end，标志master是否结束任务
	 */
	// Your definitions here.
	files     []string
	nReduce   int
	taskPhase TaskPhase
	TaskStats []TaskStat
	mu        sync.Mutex
	done      bool
	workerSeq int
	taskCh    chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.files),
		Seq:      taskSeq,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	DebugPrintf("m:%+v, taskseq:%d, lents:%d", m, taskSeq, len(m.files), len(m.TaskStats))
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	allFinish := true

	for index, t := range m.TaskStats {
		switch t.Status {
		case TaskStatusReady:
			allFinish = false
			m.taskCh <- m.getTask(index)
			m.TaskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.TaskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			m.TaskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		default:
			panic("t.status err")
		}
	}

	if allFinish {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}

}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.TaskStats = make([]TaskStat, len(m.files))
}

func (m *Master) initReduceTask() {
	DebugPrintf("init ReduceTask")
	m.taskPhase = ReducePhase
	m.TaskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task.Phase != m.taskPhase {
		panic("req task phase neq")
	}
	m.TaskStats[task.Seq].Status = TaskStatusRunning
	m.TaskStats[task.Seq].WorkerId = args.WorkerId
	m.TaskStats[task.Seq].StartTime = time.Now()
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task
	if task.Alive {
		m.regTask(args, &task)
	}
	DebugPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	DebugPrintf("get report task: %+v, taskPhase: %+v", args, m.taskPhase)
	if m.taskPhase != args.Phase || args.WorkerId != m.TaskStats[args.Seq].WorkerId {
		return nil
	}
	if args.Done {
		m.TaskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.TaskStats[args.Seq].Status = TaskStatusErr
	}
	go m.schedule()
	return nil
}

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := masterSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}
	m.initMapTask()
	go m.tickSchedule()
	m.server()
	DebugPrintf("master init")
	return &m
}
