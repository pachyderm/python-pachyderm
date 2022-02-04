Search.setIndex({docnames:["getting_started","index","modules","python_pachyderm","python_pachyderm.mixin"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":4,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":3,"sphinx.domains.rst":2,"sphinx.domains.std":2,"sphinx.ext.viewcode":1,sphinx:56},filenames:["getting_started.md","index.rst","modules.rst","python_pachyderm.rst","python_pachyderm.mixin.rst"],objects:{"python_pachyderm.client":[[3,1,1,"","Client"]],"python_pachyderm.client.Client":[[3,2,1,"","__init__"],[3,2,1,"","delete_all"],[3,2,1,"","new_from_config"],[3,2,1,"","new_from_pachd_address"],[3,2,1,"","new_in_cluster"]],"python_pachyderm.mixin":[[4,0,0,"-","admin"],[4,0,0,"-","auth"],[4,0,0,"-","debug"],[4,0,0,"-","enterprise"],[4,0,0,"-","health"],[4,0,0,"-","identity"],[4,0,0,"-","license"],[4,0,0,"-","pfs"],[4,0,0,"-","pps"],[4,0,0,"-","transaction"],[4,0,0,"-","version"]],"python_pachyderm.mixin.admin":[[4,1,1,"","AdminMixin"]],"python_pachyderm.mixin.admin.AdminMixin":[[4,2,1,"","inspect_cluster"]],"python_pachyderm.mixin.auth":[[4,1,1,"","AuthMixin"]],"python_pachyderm.mixin.auth.AuthMixin":[[4,2,1,"","activate_auth"],[4,2,1,"","authenticate_id_token"],[4,2,1,"","authenticate_oidc"],[4,2,1,"","authorize"],[4,2,1,"","deactivate_auth"],[4,2,1,"","extract_auth_tokens"],[4,2,1,"","get_auth_configuration"],[4,2,1,"","get_groups"],[4,2,1,"","get_oidc_login"],[4,2,1,"","get_robot_token"],[4,2,1,"","get_role_binding"],[4,2,1,"","get_roles_for_permission"],[4,2,1,"","get_users"],[4,2,1,"","modify_members"],[4,2,1,"","modify_role_binding"],[4,2,1,"","restore_auth_token"],[4,2,1,"","revoke_auth_token"],[4,2,1,"","set_auth_configuration"],[4,2,1,"","set_groups_for_user"],[4,2,1,"","who_am_i"]],"python_pachyderm.mixin.debug":[[4,1,1,"","DebugMixin"]],"python_pachyderm.mixin.debug.DebugMixin":[[4,2,1,"","binary"],[4,2,1,"","dump"],[4,2,1,"","profile_cpu"]],"python_pachyderm.mixin.enterprise":[[4,1,1,"","EnterpriseMixin"]],"python_pachyderm.mixin.enterprise.EnterpriseMixin":[[4,2,1,"","activate_enterprise"],[4,2,1,"","deactivate_enterprise"],[4,2,1,"","get_activation_code"],[4,2,1,"","get_enterprise_state"]],"python_pachyderm.mixin.health":[[4,1,1,"","HealthMixin"]],"python_pachyderm.mixin.health.HealthMixin":[[4,2,1,"","health_check"]],"python_pachyderm.mixin.identity":[[4,1,1,"","IdentityMixin"]],"python_pachyderm.mixin.identity.IdentityMixin":[[4,2,1,"","create_idp_connector"],[4,2,1,"","create_oidc_client"],[4,2,1,"","delete_all_identity"],[4,2,1,"","delete_idp_connector"],[4,2,1,"","delete_oidc_client"],[4,2,1,"","get_identity_server_config"],[4,2,1,"","get_idp_connector"],[4,2,1,"","get_oidc_client"],[4,2,1,"","list_idp_connectors"],[4,2,1,"","list_oidc_clients"],[4,2,1,"","set_identity_server_config"],[4,2,1,"","update_idp_connector"],[4,2,1,"","update_oidc_client"]],"python_pachyderm.mixin.license":[[4,1,1,"","LicenseMixin"]],"python_pachyderm.mixin.license.LicenseMixin":[[4,2,1,"","activate_license"],[4,2,1,"","add_cluster"],[4,2,1,"","delete_all_license"],[4,2,1,"","delete_cluster"],[4,2,1,"","get_activation_code"],[4,2,1,"","list_clusters"],[4,2,1,"","list_user_clusters"],[4,2,1,"","update_cluster"]],"python_pachyderm.mixin.pfs":[[4,1,1,"","ModifyFileClient"],[4,1,1,"","PFSFile"],[4,1,1,"","PFSMixin"],[4,1,1,"","PFSTarFile"]],"python_pachyderm.mixin.pfs.ModifyFileClient":[[4,2,1,"","copy_file"],[4,2,1,"","delete_file"],[4,2,1,"","put_file_from_bytes"],[4,2,1,"","put_file_from_fileobj"],[4,2,1,"","put_file_from_filepath"],[4,2,1,"","put_file_from_url"]],"python_pachyderm.mixin.pfs.PFSFile":[[4,2,1,"","close"],[4,2,1,"","read"]],"python_pachyderm.mixin.pfs.PFSMixin":[[4,2,1,"","commit"],[4,2,1,"","copy_file"],[4,2,1,"","create_branch"],[4,2,1,"","create_repo"],[4,2,1,"","delete_all_repos"],[4,2,1,"","delete_branch"],[4,2,1,"","delete_file"],[4,2,1,"","delete_repo"],[4,2,1,"","diff_file"],[4,2,1,"","drop_commit"],[4,2,1,"","finish_commit"],[4,2,1,"","fsck"],[4,2,1,"","get_file"],[4,2,1,"","get_file_tar"],[4,2,1,"","glob_file"],[4,2,1,"","inspect_branch"],[4,2,1,"","inspect_commit"],[4,2,1,"","inspect_file"],[4,2,1,"","inspect_repo"],[4,2,1,"","list_branch"],[4,2,1,"","list_commit"],[4,2,1,"","list_file"],[4,2,1,"","list_repo"],[4,2,1,"","modify_file_client"],[4,2,1,"","path_exists"],[4,2,1,"","put_file_bytes"],[4,2,1,"","put_file_url"],[4,2,1,"","squash_commit"],[4,2,1,"","start_commit"],[4,2,1,"","subscribe_commit"],[4,2,1,"","wait_commit"],[4,2,1,"","walk_file"]],"python_pachyderm.mixin.pps":[[4,1,1,"","PPSMixin"]],"python_pachyderm.mixin.pps.PPSMixin":[[4,2,1,"","create_pipeline"],[4,2,1,"","create_pipeline_from_request"],[4,2,1,"","create_secret"],[4,2,1,"","delete_all_pipelines"],[4,2,1,"","delete_job"],[4,2,1,"","delete_pipeline"],[4,2,1,"","delete_secret"],[4,2,1,"","get_job_logs"],[4,2,1,"","get_pipeline_logs"],[4,2,1,"","inspect_datum"],[4,2,1,"","inspect_job"],[4,2,1,"","inspect_pipeline"],[4,2,1,"","inspect_secret"],[4,2,1,"","list_datum"],[4,2,1,"","list_job"],[4,2,1,"","list_pipeline"],[4,2,1,"","list_secret"],[4,2,1,"","restart_datum"],[4,2,1,"","run_cron"],[4,2,1,"","start_pipeline"],[4,2,1,"","stop_job"],[4,2,1,"","stop_pipeline"]],"python_pachyderm.mixin.transaction":[[4,1,1,"","TransactionMixin"]],"python_pachyderm.mixin.transaction.TransactionMixin":[[4,2,1,"","batch_transaction"],[4,2,1,"","delete_all_transactions"],[4,2,1,"","delete_transaction"],[4,2,1,"","finish_transaction"],[4,2,1,"","inspect_transaction"],[4,2,1,"","list_transaction"],[4,2,1,"","start_transaction"],[4,2,1,"","transaction"]],"python_pachyderm.mixin.version":[[4,1,1,"","VersionMixin"]],"python_pachyderm.mixin.version.VersionMixin":[[4,2,1,"","get_remote_version"]],"python_pachyderm.pfs":[[3,1,1,"","Commit"],[3,4,1,"","commit_from"]],"python_pachyderm.pfs.Commit":[[3,3,1,"","branch"],[3,2,1,"","from_pb"],[3,3,1,"","id"],[3,3,1,"","repo"],[3,3,1,"","repo_type"],[3,2,1,"","to_pb"]],"python_pachyderm.util":[[3,4,1,"","parse_dict_pipeline_spec"],[3,4,1,"","parse_json_pipeline_spec"],[3,4,1,"","put_files"]],python_pachyderm:[[3,0,0,"-","client"],[4,0,0,"-","mixin"],[3,0,0,"-","pfs"],[3,0,0,"-","util"]]},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","attribute","Python attribute"],"4":["py","function","Python function"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:attribute","4":"py:function"},terms:{"0":[3,4],"1":[0,3,4],"12345":3,"1658":4,"17":3,"172":3,"2":[3,4],"3":[0,3,4],"30650":[0,3],"467c580611234cdb8cc9758c7aa96087":4,"6":[0,3,4],"6fe754facd9c41e99d04e1037e3be9e":4,"80":3,"9":[],"break":4,"byte":[3,4],"case":4,"class":[3,4],"default":[3,4],"do":[0,4],"enum":4,"function":[3,4],"import":[0,3],"int":[3,4],"long":4,"new":[0,4],"public":4,"return":[0,3,4],"short":[],"static":3,"true":[3,4],"var":3,"while":4,A:[0,3,4],As:[],For:[0,4],If:[1,3,4],In:4,Is:4,It:4,Or:[],The:[3,4],These:4,To:[0,3],_:4,__init__:3,a3ak09467c580611234cdb8cc9758c7a:4,about:[0,4],abov:[0,4],absolut:4,access:4,acl:4,aclentri:[],act:[],action:4,activ:4,activate_auth:4,activate_enterpris:4,activate_licens:4,activation_cod:4,active_context:3,active_transact:3,ad:4,add:[0,3,4],add_clust:4,addclusterrespons:4,addit:[],address:[3,4],admin:[2,3],admin_proto:4,administ:[],adminmixin:[3,4],affect:[],aforement:4,against:4,agnost:4,alia:[3,4],aliv:[],all:[3,4],alloc:4,allow:4,allow_incomplet:[],allow_just_repo:[],alphanumer:4,alreadi:4,also:[],altern:[],amount:4,an:[3,4],analysi:0,ancestor:4,ani:[0,4],annot:4,anoth:4,api:0,appear:4,append:4,appli:4,approxim:[],ar:[3,4],arg:4,argument:3,around:[],arrai:[],asset:0,associ:[],assum:4,atom:4,atomicop:[],atomicputfileobjop:[],atomicputfilepathop:[],attach:4,attempt:4,attribut:[3,4],audienc:4,auth:[2,3],auth_proto:4,auth_token:3,authconfig:[],authent:[3,4],authenticate_github:[],authenticate_id_token:4,authenticate_oidc:4,authenticate_one_time_password:[],authmixin:[3,4],author:4,authorizerespons:4,auto:4,autom:1,automat:[0,4],autosc:4,avail:4,avoid:4,b:[0,3,4],back:[0,4],backend:4,backup:[],bake:[],bar:[3,4],bargroup:4,base:[3,4],bash:0,batch:4,batch_transact:4,baz:[3,4],becaus:4,becom:4,been:4,befor:4,begin:4,behav:[],behavior:3,being:4,belong:4,below:[],better:4,between:4,binari:4,binaryio:4,bind:4,blob:4,block:4,block_stat:[],bool:[3,4],both:[3,4],branch:[0,3,4],branch_nam:4,branchinfo:4,broken:[],bucket:4,buffer:4,build:[],builder:[],built:4,bystr:4,bytearrai:4,bytestr:4,c1:4,c2:4,c:4,cache_s:[],call:[0,3,4],caller:[],can:[0,3,4],cancel:[],cannot:4,care:4,caus:4,cert:3,certain:4,certif:3,certifi:3,chain:[],chang:4,channel:[],charact:4,check:[0,1,3,4],chunk:[],chunk_spec:[],chunkspec:[],circumv:[],classmethod:3,client:[0,1,2,4],client_id:4,client_secret:4,close:4,cluster:[0,3,4],cluster_deployment_id:4,clusteradmin:4,clusterinfo:4,clusterrol:[],clusterstatu:4,cmd:[0,3,4],code:4,coerc:4,collect:[],com:[3,4],command:4,commit2:3,commit:[0,3,4],commit_from:3,commit_id:[3,4],commit_st:4,commitinfo:4,commitproven:[],commitsetinfo:4,commitst:4,compar:4,compat:[],complet:4,comput:[],concept:4,condit:4,config:[3,4],config_fil:3,configur:4,confirm:[],conjunct:4,connect:[0,3,4],connector:4,consid:[],consist:4,consum:4,contain:[3,4],content:[3,4],context:[3,4],continu:[0,4],control:1,conveni:[],convert:3,copi:4,copy_fil:4,copybufs:4,copyfileobj:4,core:[],correct:4,correspond:[],cost:[],count:0,cours:4,cpu:4,creat:[0,3,4],create_branch:4,create_idp_connector:4,create_oidc_cli:4,create_pipelin:[0,4],create_pipeline_from_request:[3,4],create_python_pipelin:[],create_repo:[0,4],create_secret:4,create_tf_job_pipelin:[],create_tmp_file_set:[],createbranchrequest:4,createpipelinerequest:[3,4],creation:4,credenti:4,criteria:4,cron:4,cross:4,csv:[0,3],current:4,custom:[],d:3,dash:[],data1:4,data2:[0,4],data:[0,1,3,4],data_filt:4,databas:4,datum:4,datum_id:4,datum_object_hash:[],datum_set_spec:4,datum_timeout:4,datum_tri:4,datuminfo:4,datumsetspec:4,deactiv:4,deactivate_auth:4,deactivate_enterpris:4,debug:[2,3],debug_proto:4,debugmixin:[3,4],delet:[3,4],delete_al:3,delete_all_ident:4,delete_all_licens:4,delete_all_pipelin:4,delete_all_repo:4,delete_all_transact:4,delete_branch:4,delete_clust:4,delete_commit:[],delete_fil:4,delete_idp_connector:4,delete_job:4,delete_m:4,delete_oidc_cli:4,delete_pipelin:4,delete_repo:4,delete_secret:4,delete_transact:4,deletefil:[],deletereporequest:4,delimit:[],demand:[],depend:[],deploi:[],deploy:4,derefer:4,descend:4,descript:[3,4],desir:0,dest_commit:4,dest_fil:4,dest_path:[3,4],destin:[3,4],detail:[3,4],detect:3,determin:[],df:0,dict:[3,4],dictionari:[3,4],diff:4,diff_fil:4,differ:4,difffilerespons:4,dir:4,dir_a:0,directli:4,directori:[0,3,4],doc:[0,3,4],docker:[],document:[3,4],doe:[],doesn:4,don:[3,4],download:0,driven:0,drop:4,drop_commit:4,due:[],dump:4,durat:4,duration_pb2:4,dure:4,e:[],each:4,earlier:4,edg:3,effect:[],effici:4,egress:4,either:4,embed:4,empti:4,en:4,enabl:[3,4],enable_stat:[],encod:[3,4],encount:4,end:[1,4],endless:4,enqueu:4,ensur:4,enterpris:[2,3],enterprise_proto:4,enterprise_serv:4,enterprisemixin:[3,4],entir:4,entri:[],env:3,environ:[],equival:3,ergonom:[],error:4,errorlevel:4,establish:4,etc:4,etcdpipelineinfo:[],even:4,evenli:[],everyth:[],evict:4,exactli:4,exampl:[1,3,4],exce:4,exclud:4,exclus:4,execut:4,exist:[3,4],exit:4,expir:4,expiri:4,explicitli:4,expos:4,extend:[],extend_auth_token:[],extern:4,extract:4,extract_auth_token:4,extract_pipelin:[],f:[0,4],fail:4,failur:4,fals:4,familiar:1,far:4,faster:4,favorit:0,feel:4,fetch:4,fewer:[],field:[3,4],file:[0,3,4],fileinfo:4,fileish:[],fileobj:4,fileobject:4,fileset:[],fileset_id:[],filter:4,find:4,finish:[0,4],finish_commit:4,finish_transact:4,finishcommit:4,first:1,fix:4,flush:[],flush_commit:[],flush_job:[],flushcommit:[],folder:0,follow:4,foo:[3,4],foobar:[3,4],foogroup:4,forc:4,format:4,forward:3,found:4,free:4,frequenc:0,from:[0,3,4],from_commit:4,from_commit_id:[],from_pb:3,fsck:4,fsckrespons:4,full:[],futur:4,g:[],garbag:[],garbage_collect:[],gatewai:4,gener:4,get:[1,4],get_acl:[],get_activation_cod:4,get_admin:[],get_auth_configur:4,get_auth_token:[],get_cluster_role_bind:[],get_enterprise_st:4,get_fil:[0,4],get_file_tar:4,get_group:4,get_identity_server_config:4,get_idp_connector:4,get_job_log:4,get_oidc_cli:4,get_oidc_login:4,get_one_time_password:[],get_pipeline_log:4,get_remote_vers:4,get_robot_token:4,get_role_bind:4,get_roles_for_permiss:4,get_scop:[],get_us:4,getaclrespons:[],getactivationcoderespons:4,getauthtokenrespons:[],getenterpriserespons:[],getoidcloginrespons:4,getstaterespons:4,giant:4,git:4,github:4,github_token:[],give:4,given:[3,4],glob:[0,3,4],glob_fil:4,go:[3,4],googl:4,grant:4,grep:0,group:4,grpc:[3,4],ha:4,had:4,handl:4,happen:4,hash:4,hashtree_spec:[],hashtreespec:[],have:4,haven:4,head:4,head_commit:4,header:[],header_record:[],health:[2,3],health_check:4,health_proto:4,healthcheckrespons:4,healthmixin:[3,4],hello:1,help:[],helper:[1,2],here:[0,4],higher:0,histor:4,histori:4,hold:4,host:3,how:4,html:4,http:[3,4],hub:0,hyperparam:3,hypothet:4,i:[],id:[0,3,4],id_token:4,ident:[2,3],identifi:[3,4],identity_proto:4,identitymixin:[3,4],identityserverconfig:4,idp:4,idpconnector:4,ignore_zero:4,imag:[3,4],image_pull_secret:[],immut:4,impact:4,includ:4,include_cont:[],incomplet:[],index:1,indic:4,individu:4,info:[3,4],inform:[2,3],init:0,initi:3,input:[0,3,4],input_commit:4,input_tree_object_hash:[],insert:3,inspect:4,inspect_branch:4,inspect_clust:4,inspect_commit:4,inspect_datum:4,inspect_fil:4,inspect_job:4,inspect_pipelin:4,inspect_repo:4,inspect_secret:4,inspect_transact:4,instal:1,instanc:3,instead:4,intact:4,interact:[1,3],intern:4,intuit:[],io:[3,4],irrevoc:4,is_tar:[],isn:4,issu:4,issuer:4,iter:4,its:[1,3,4],itself:3,j:3,job:[3,4],job_id:4,job_timeout:4,jobinfo:4,jobsetinfo:4,joint_data:4,jq:4,jqfilter:4,json:3,just:[],keep:4,keep_repo:4,kei:4,keyword:3,kubeflow:[],kubernet:4,kwarg:3,l27:4,label:4,lack:4,languag:[],larg:4,larger:[],last:[],later:4,latest:[3,4],lazili:4,learn:0,least:4,leav:4,left:[],let:4,level:[0,4],librari:3,licens:[2,3],license_proto:4,license_serv:4,licensemixin:[3,4],lieu:[],lifetim:4,like:[0,3,4],limit:4,line:4,linear:[],list:4,list_branch:4,list_clust:4,list_commit:4,list_datum:4,list_fil:4,list_idp_connector:4,list_job:4,list_oidc_cli:4,list_pipelin:4,list_repo:4,list_secret:4,list_transact:4,list_user_clust:4,listdatumstreamrespons:[],listen:4,live:[],ll:[],load:3,local:[0,3,4],local_path:4,localhost:[0,3,4],locat:[],log:[0,4],login:4,logmessag:4,loki:4,look:4,lower:[],mai:4,main:4,make:[0,4],manag:4,mani:4,manual:3,map:4,marker:[],marker_filenam:[],master:[0,3,4],match:4,matter:[],max_queue_s:[],maximum:4,mean:4,member:4,membership:4,memori:4,memory_byt:[],merg:[],messag:4,meta:4,metadata:[3,4],method:[3,4],metric:4,mfc:4,might:4,migrat:4,minim:[],mixin:[1,2],mode:4,modifi:4,modify_admin:[],modify_cluster_role_bind:[],modify_file_cli:4,modify_memb:4,modify_role_bind:4,modifyfil:4,modifyfilecli:[3,4],modul:[1,3],montag:4,montage2:4,more:[0,3,4],most:4,move:4,much:4,multipl:4,must:4,my_branch:0,my_data:0,my_repo:0,name:[3,4],namedtupl:3,necessari:3,need:4,never:[],new_commit:4,new_fil:4,new_from_config:3,new_from_pachd_address:3,new_in_clust:3,new_path:4,newer:4,newest:4,newli:4,next:4,no_auth:[],no_enterpris:[],no_object:[],no_pipelin:[],no_repo:[],non:[],none:[3,4],nonzero:4,noqa:[],normal:4,note:4,now:4,number:[3,4],object:[0,3,4],obtain:4,occur:4,occurr:0,offici:[],offset:4,offset_byt:[],oidc:4,oidc_stat:4,oidcclient:4,oidcconfig:4,old:[],old_commit:4,old_path:4,older:4,oldest:4,omit:[],onc:4,one:4,one_time_password:[],ones:4,onli:4,onward:4,op:[],open:[3,4],opencv:3,oper:[3,4],option:[0,3,4],order:4,organ:4,origin:4,origin_kind:4,originkind:4,other:[0,4],otherus:4,otherwis:[3,4],our:[],out:[0,1,4],output:[0,4],output_branch:[],output_branch_nam:4,output_commit:[],outsid:[],over:4,overwrit:4,overwrite_index:[],p:4,pach:4,pach_config:3,pachctl:[0,3],pachd:[3,4],pachd_address:3,pachdyerm:4,pachyderm:[3,4],packag:0,page:1,page_s:[],panda:0,parallel:4,parallelism_spec:4,parallelismspec:4,param:[3,4],paramet:[3,4],parent:4,pars:3,parse_dict_pipeline_spec:[3,4],parse_json_pipeline_spec:[3,4],pass:0,password:[],past:4,path:[3,4],path_exist:4,path_to:0,pattern:4,pax_head:4,pd:0,peer:4,pem:3,per:4,perform:[3,4],permiss:4,permit:[],persist:4,person_a:4,pf:[0,1,2],pfs_directori:[],pfs_path:4,pfs_proto:[3,4],pfsfile:4,pfsinput:[0,4],pfsmixin:[3,4],pfstarfil:4,pip:0,pipe:[],pipelin:[1,3,4],pipeline_input:[],pipeline_kwarg:[],pipeline_nam:4,pipeline_spec:[3,4],pipelineinfo:4,pleas:4,png:4,pod:4,pod_patch:4,point:4,pool:4,port:3,possibl:4,potenti:4,pp:[2,3],pps_proto:[0,3,4],ppsmixin:[3,4],precis:[],prefix:[],prerequisit:1,present:[],preserv:[],prevent:4,previou:[],primarili:3,princip:4,principt:4,print:[0,4],privat:[],problem:4,probotuf:4,process:4,profil:4,profile_cpu:4,project:4,propag:[],properti:[],proto:4,protobuf:[3,4],prov:[],proven:4,provid:[3,4],publicli:4,publish:4,pull:[],pure:[],purpos:[],push:[],put:[0,3,4],put_fil:3,put_file_byt:[0,4],put_file_cli:[],put_file_from_byt:4,put_file_from_fileobj:4,put_file_from_fileobj_req:[],put_file_from_filepath:[3,4],put_file_from_iterable_req:[],put_file_from_url:4,put_file_url:4,put_marker_from_byt:[],put_marker_from_fileobj:[],putfil:[],putfilecli:4,putfilerequest:[],py:[3,4],pypi:0,python3:[3,4],python:[0,4],python_pachyderm:[0,1,2],queri:[3,4],queue:4,r:[3,4],random:4,rather:4,re:[1,4],reach:4,read:[0,4],read_csv:0,readi:4,real:4,reason:4,recent:4,reclaim:4,recommend:0,record:[],recurs:[3,4],redirect:4,redirect_uri:4,refer:[3,4],regardless:4,regist:4,registri:[],relat:4,relationship:4,remain:4,remov:4,renew:[],renew_tmp_file_set:[],replac:4,replic:3,repo:[0,1,3,4],repo_nam:[3,4],repo_read:4,repo_typ:3,repoinfo:4,repoown:4,repowrit:4,repres:[3,4],represent:3,reprocess:4,reprocess_spec:4,req:[3,4],request:4,requir:4,reset:3,resourc:4,resource_limit:4,resource_request:4,resourcespec:4,rest:4,restart:4,restart_datum:4,restor:4,restore_auth_token:4,restorerequest:[],restrict:[],result:[0,4],retri:[],retriev:[],revers:4,revok:4,revoke_auth_token:4,robot:4,roh:0,role:4,root:[0,3,4],root_cert:3,root_token:4,roughli:3,row:[],rpc:4,run:[0,3,4],run_cron:4,run_pipelin:[],runtim:[],s3:4,s3_out:4,s:[3,4],safe:[],safer:4,salt:4,same:4,satisfi:4,scale:4,scale_down_threshold:[],schedul:4,scheduling_spec:4,schedulingspec:4,scienc:1,scope:4,scrape:4,search:[1,3,4],second:4,secret:4,secret_nam:4,secretinfo:4,secur:4,see:[0,3,4],semant:4,sent:4,separ:4,sequenc:4,serial:3,server:[3,4],server_ca:3,servic:[0,4],session_token:3,set:[3,4],set_acl:[],set_auth_configur:4,set_groups_for_us:4,set_identity_server_config:4,set_scop:[],sever:4,shallow:4,shard:[],share:4,should:[3,4],shouldn:[],shutil:4,sibl:[],sidecar:4,sidecar_resource_limit:4,sign:4,significantli:4,silent:4,similar:[0,4],sinc:[0,4],singl:4,size:4,size_byt:[],skip:[],sleep:[],so:4,some:4,someus:4,sophist:0,sourc:[0,3,4],source_commit:4,source_dir:3,source_fil:4,source_path:[3,4],space:4,spec:[3,4],spec_commit:4,special:[],specif:4,specifi:[3,4],split:4,split_transact:[],spout:4,spoutcommit:[],spoutmanag:[],sql:[],squash:4,squash_commit:4,src:4,stage:4,standbi:[],start:[1,4],start_commit:4,start_pipelin:4,start_transact:4,state:[3,4],statu:4,status_onli:[],stdin:0,step:[],still:4,stop:4,stop_job:4,stop_pipelin:4,storag:4,store:4,str:[3,4],stream:4,string:[3,4],stringio:3,stuff:4,subclass:3,subcommit:4,subdir:4,subject:[],subjob:4,subscrib:[],subscribe_commit:4,superus:4,support:4,surrogateescap:4,system:4,t:[3,4],tag:4,tail:4,target:[],target_file_byt:[],target_file_datum:[],tarinfo:4,temp:[],temporari:[],test:[0,4],textio:3,tf_job:[],tfjob:[],than:4,thei:[3,4],them:4,themselv:4,thi:[0,3,4],those:4,threshold:4,through:4,ti:4,time:[0,4],timestamp:4,timestamp_pb2:4,tl:3,to_commit:4,to_pb:3,token:[3,4],tokeninfo:4,tombston:4,too:4,tool:1,top:[0,4],total:4,train:[3,4],training_set:3,transact:[2,3],transaction_from:[],transaction_id:3,transaction_proto:4,transaction_protobuf:4,transactioninfo:4,transactionmixin:[3,4],transactionrequest:4,transform:[0,3,4],treat:[],tree:[],tree_object_hash:[],trial:4,trigger:[0,4],trust:4,ttl:4,ttl_second:[],tupl:[3,4],two:[3,4],txt:[0,4],type:4,unabl:4,under:[0,4],unfinish:4,union:[3,4],uniqu:4,unless:4,unset:[3,4],unspecifi:3,until:4,unus:4,up:[],updat:4,update_clust:4,update_idp_connector:4,update_oidc_cli:4,upload:4,upon:4,upper:4,upsert:[],upstream:4,uri:4,url:4,us:[3,4],usag:[],use_default_host:3,use_loki_backend:4,user:[3,4],user_address:4,userclusterinfo:4,usernam:4,usual:4,util:[1,2,4],v2:3,valid:4,valu:[1,4],variabl:[],variant:[],version:[0,1,2,3],version_proto:4,versionmixin:[3,4],via:[0,4],w505:[],w:0,wa:4,wai:[],wait:[0,4],wait_commit:[0,4],walk:4,walk_fil:4,want:4,wasn:4,wb:4,wc:0,we:[3,4],web:4,were:[],what:[0,4],when:4,where:[],whether:[3,4],which:[0,3,4],who_am_i:4,whoamirespons:4,whose:4,wide:[],within:[3,4],without:[],word:[0,4],word_count:0,work:[],worker:4,workspac:0,world:1,would:4,write:4,written:4,x:4,yield:4,yo:[],you:[0,1,4],your:[0,4],zero:[]},titles:["Getting Started","python-pachyderm","&lt;no title&gt;","python_pachyderm","Information"],titleterms:{admin:4,auth:4,client:3,content:[],debug:4,doc:1,document:[],enterpris:4,exampl:0,get:0,health:4,hello:0,helper:3,ident:4,indic:1,inform:4,instal:0,licens:4,link:1,mixin:[3,4],modul:[],overview:1,pachyderm:[0,1],pf:[3,4],pipelin:0,pp:4,prerequisit:0,python:1,python_pachyderm:[3,4],s:[],spout:[],start:0,tabl:1,transact:4,util:3,version:4,welcom:[],world:0}})