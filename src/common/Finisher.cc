// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Finisher.h"

#define dout_subsys ceph_subsys_finisher
#undef dout_prefix
#define dout_prefix *_dout << "finisher(" << this << ") "

#include "include/object.h"
#include "include/buffer.h"
#include "common/ceph_time.h"
#include "osd/PrimaryLogPG.h"
#include "osdc/Objecter.h"
#include <map>



void Finisher::start()
{
  ldout(cct, 10) << __func__ << dendl;
  finisher_thread.create(thread_name.c_str());
}

void Finisher::stop()
{
  ldout(cct, 10) << __func__ << dendl;
  finisher_lock.lock();
  finisher_stop = true;
  // we don't have any new work to do, but we want the worker to wake up anyway
  // to process the stop condition.
  finisher_cond.notify_all();
  finisher_lock.unlock();
  finisher_thread.join(); // wait until the worker exits completely
  ldout(cct, 10) << __func__ << " finish" << dendl;
}

void Finisher::wait_for_empty()
{
  std::unique_lock ul(finisher_lock);
  while (!finisher_queue.empty() || finisher_running) {
    ldout(cct, 10) << "wait_for_empty waiting" << dendl;
    finisher_empty_wait = true;
    finisher_empty_cond.wait(ul);
  }
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
  finisher_empty_wait = false;
}

void *Finisher::finisher_thread_entry()
{
  std::unique_lock ul(finisher_lock);
  ldout(cct, 10) << "finisher_thread start" << dendl;

  utime_t start;
  uint64_t count = 0;
  while (!finisher_stop) {
    /// Every time we are woken up, we process the queue until it is empty.
    while (!finisher_queue.empty()) {
      // To reduce lock contention, we swap out the queue to process.
      // This way other threads can submit new contexts to complete
      // while we are working.
      in_progress_queue.swap(finisher_queue);
      finisher_running = true;
      ul.unlock();
      ldout(cct, 10) << "finisher_thread doing " << in_progress_queue << dendl;

      if (logger) {
	start = ceph_clock_now();
	count = in_progress_queue.size();
      }

      // Now actually process the contexts.
      for (auto p : in_progress_queue) {
	p.first->complete(p.second);
      }
      ldout(cct, 10) << "finisher_thread done with " << in_progress_queue
                     << dendl;
      in_progress_queue.clear();
      if (logger) {
	logger->dec(l_finisher_queue_len, count);
	logger->tinc(l_finisher_complete_lat, ceph_clock_now() - start);
      }

      ul.lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_thread empty" << dendl;
    if (unlikely(finisher_empty_wait))
      finisher_empty_cond.notify_all();
    if (finisher_stop)
      break;
    
    ldout(cct, 10) << "finisher_thread sleeping" << dendl;
    finisher_cond.wait(ul);
  }
  // If we are exiting, we signal the thread waiting in stop(),
  // otherwise it would never unblock
  finisher_empty_cond.notify_all();

  ldout(cct, 10) << "finisher_thread stop" << dendl;
  finisher_stop = false;
  return 0;
}


void Cache_IO_Finisher::wait_for_empty()
{
  std::unique_lock ul(finisher_lock);
  while (!finisher_IO_queue.empty() || finisher_running) {
    ldout(cct, 10) << "wait_for_empty waiting" << dendl;
    finisher_empty_wait = true;
    finisher_empty_cond.wait(ul);
  }
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
  finisher_empty_wait = false;
}

void *Cache_IO_Finisher::finisher_thread_entry()
{
  std::unique_lock ul(finisher_lock);
  ldout(cct, 10) << "finisher_IO_thread start" << dendl;
  std::cout << "finisher_IO_thread start" << std::endl;
  utime_t start;
  uint64_t count = 0;

  std::map<uint32_t, const char*> macro_map;
  #define GET_OSD_OPS_NAME(op, opcode, str)    macro_map[opcode] = str;
  __CEPH_FORALL_OSD_OPS(GET_OSD_OPS_NAME);

while (!finisher_stop) {
    /// Every time we are woken up, we process the queue until it is empty.
   ldout(cct, 10) << "finisher_IO_thread prepare to do..." << dendl; 
    while (!finisher_IO_queue.empty()) {
      // To reduce lock contention, we swap out the queue to process.
      // This way other threads can submit new contexts to complete
      // while we are working.
      in_progress_IO_queue.swap(finisher_IO_queue);
      finisher_running = true;
      ul.unlock();
      ldout(cct, 10) << "finisher_IO_thread doing " << in_progress_IO_queue << dendl;

      if (logger) {
	start = ceph_clock_now();
	count = in_progress_IO_queue.size();
      }

      // Now actually process the contexts.
      for (auto IOtuple : in_progress_IO_queue) {
        const object_t *oid = std::get<1>(IOtuple);         // 获取对象ID
        ObjectOperation* oop = std::get<2>(IOtuple);  // 获取具体操作
        int rwmode = std::get<3>(IOtuple);           // 获取读写模式
	ldout(cct, 10) << __func__ << ", oid:" << oid->name << ".ObjectOperation addr: " << oop << "ops length:" << oop->ops.size() << dendl;
	
	int i = 0;
	auto r = oop->out_rval.begin();
	auto out = oop->out_bl.begin();
	int result = 0;
        for(auto p = oop->ops.begin(); p != oop->ops.end(); p++, r++, out++){
	  OSDOp& osd_op = *p;

          ceph_osd_op& cop = osd_op.op;
	  
          ldout(cct, 10) << "result_addr:" << *r << dendl;
	  ldout(cct, 10) << "OSDop:" << macro_map[cop.op] << dendl;

	  if(cop.op == CEPH_OSD_OP_COPY_GET ){
	    bufferlist* bl = oop->cop_data;
	    std::string filepath = "/root/ceph_data/" + oid->name;
	    std::string error;
	    result = bl->read_file(filepath.c_str(), &error);
	    ldout(cct, 10) << "CEPH_OSD_OP_COPY_GET error:" << error << dendl; 
	    ceph_assert(oop->cursor);
	    
	    oop->cursor->data_offset = bl->length();
	    oop->results->object_size = bl->length();
	    oop->results->mtime = ceph::real_clock::now();
	    oop->results->truncate_seq = cop.extent.truncate_seq;
	    oop->results->truncate_size = cop.extent.truncate_size;

	    oop->cursor->attr_complete = true;
	    oop->cursor->data_complete = true;
            oop->cursor->omap_complete = true;
	  }
	
	  else if(cop.op == CEPH_OSD_OP_COPY_FROM){
	    bufferlist& bl = osd_op.indata;
	    std::string filepath = "/root/ceph_data/" + oid->name;
	    result = bl.write_file(filepath.c_str(), 0755);
	    bl.clear();
	  }

	  else if ((cop.op == CEPH_OSD_OP_READ) 
			  | (cop.op == CEPH_OSD_OP_SYNC_READ) 
			  | (cop.op == CEPH_OSD_OP_SPARSE_READ)
			  | (cop.op == CEPH_OSD_OP_CHECKSUM) ){
	    bufferlist& bl = **out;
	    auto length = cop.extent.length;
	    auto offset = cop.extent.offset;
	    std::string filepath = "/root/ceph_data/" + oid->name;
	    std::string error;
	    result = bl.pread_file(filepath.c_str(), offset, length, &error);
	    ldout(cct, 10) << "CEPH_OSD_OP_READ buffer:" << bl.c_str() << " error:" << error << dendl; 
	  }

	  else if ((cop.op == CEPH_OSD_OP_WRITE) 
			  | (cop.op == CEPH_OSD_OP_WRITEFULL)){
	    bufferlist& bl = osd_op.indata;
	    auto length = cop.extent.length;
	    auto offset = cop.extent.offset;
	    if (length != bl.length())
		result = -EINVAL;
	    else{
	      std::string filepath = "/root/ceph_data/" + oid->name;
	      int fd = ::open(filepath.c_str(), O_WRONLY|O_CREAT|O_CLOEXEC, 0755);
	      result = bl.write_fd(fd, offset);
	      ::close(fd);
	      bl.clear();    
	    }
	  }
	  else if(cop.op == CEPH_OSD_OP_DELETE){
      		std::string filepath = "/root/ceph_data/" + oid->name;
	    	result = ::unlink(filepath.c_str());
	  }

	  // 设置返回值
	  if(*r){
	    **r = result;
	    ldout(cct, 10) << "IO result:" << result << dendl;
	  }
	  if(i++ < 100){
	    ldout(cct, 10) << i << ":IO length:" << cop.extent.length << dendl;
	    ldout(cct, 10) << i << ":IO offset:" << cop.extent.offset << dendl;
	    ldout(cct, 10) << i << ":IO truncate_size:" << cop.extent.truncate_size << dendl;
	    ldout(cct, 10) << i << ":IO truncate_seq:" << cop.extent.truncate_seq << dendl;
	  }

	  ldout(cct, 10) << "finish one IO!" << dendl;
	}
	
	ldout(cct, 10) << "finish one OPS!" << dendl;
	auto callback = std::get<0>(IOtuple);

	ldout(cct, 10) << "callback:" << callback << dendl;
	delete oop;	
	if(callback){
          callback->complete(result);
	}
      }
      ldout(cct, 10) << "finisher_IO_thread done with " << in_progress_IO_queue
                     << dendl;
      in_progress_IO_queue.clear();
      if (logger) {
	logger->dec(l_finisher_queue_len, count);
	logger->tinc(l_finisher_complete_lat, ceph_clock_now() - start);
      }

      ul.lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_IO_thread empty" << dendl;
    if (unlikely(finisher_empty_wait))
      finisher_empty_cond.notify_all();
    if (finisher_stop)
      break;
    
    ldout(cct, 10) << "finisher_IO_thread sleeping" << dendl;
    finisher_cond.wait(ul);
  }
  // If we are exiting, we signal the thread waiting in stop(),
  // otherwise it would never unblock
  finisher_empty_cond.notify_all();

  ldout(cct, 10) << "finisher_IO_thread stop" << dendl;
  finisher_stop = false;
  return 0;
}
