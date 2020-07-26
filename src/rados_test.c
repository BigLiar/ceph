#include <rados/librados.h>
#include <stdio.h>
#include <stdlib.h>

rados_t cluster;
rados_ioctx_t io;


void rados_IO_finish(rados_completion_t completion, char* buffer){
	printf("start callback!\n");
	int32_t error_code  = rados_aio_get_return_value(completion);
	rados_aio_release(completion);
	if(error_code < 0){
		fprintf(stderr, "callback: cannot read: %s\n", strerror(-error_code));
		free(buffer);
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}
	printf("%s", buffer);
	free(buffer);
}

int32_t main(int32_t argc, const char* argv[]) {
	int32_t error_code = 0;
	char cluster_name[] = "ceph";
	char user_name[] = "client.admin";

	error_code = rados_create(&cluster, NULL);
	if (error_code < 0) {
		fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-error_code));
		exit(1);
	}


	error_code = rados_conf_read_file(cluster, "/root/ceph/build/ceph.conf");

	if (error_code < 0) {
		fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-error_code));
		exit(1);
	}


	error_code = rados_connect(cluster);
	if (error_code < 0) {
		fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-error_code));
		exit(1);
	}


	/* Continued from previous C example, where cluster handle and
	 *      *          * connection are established. First declare an I/O Context.
	 *           *                   */

	const char *poolname = "device_health_metrics";


	error_code = rados_ioctx_create(cluster, poolname, &io);
	if (error_code < 0) {
		fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-error_code));
		rados_shutdown(cluster);
		exit(1);
	}	

	
	// Write_full an object synchronously.
	

	/*
	error_code = rados_write_full(io, "hello.txt", "hello world!\0", 13);
	if (error_code < 0) {
		fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-error_code));
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}


	// Write an object synchronously. 


	error_code = rados_write(io, "hello.txt", "aa", 2, 2);
	if (error_code < 0) {
		fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-error_code));
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}
	*/

	//Read an object synchronously.
	
	char* read_buffer = malloc(13);
	
	/*
	error_code = rados_read(io, "hello.txt", read_buffer, 13, 0);
	if (error_code < 0) {
		fprintf(stderr, "%s: cannot read pool %s: %s\n", argv[0], poolname, strerror(-error_code));
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}
	printf("%s", read_buffer);
	free(read_buffer);
	rados_ioctx_destroy(io);
	rados_shutdown(cluster);


	*/

	/* Read an object asynchronously. */
	rados_completion_t completion;
	error_code = rados_aio_create_completion2
		(read_buffer, (rados_callback_t)rados_IO_finish, &completion);
	if(error_code < 0){
		fprintf(stderr, "%s: cannot read pool %s: %s\n", argv[0], poolname, strerror(-error_code));
		free(read_buffer);
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}

	error_code = rados_aio_read(io, "hello.txt", completion, read_buffer, 13, 0);
	if(error_code < 0){
		fprintf(stderr, "%s: cannot read pool %s: %s\n", argv[0], poolname, strerror(-error_code));
		free(read_buffer);
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}
	printf("%s\n", read_buffer);
	rados_aio_wait_for_complete(completion); 	
	return 0;
}
