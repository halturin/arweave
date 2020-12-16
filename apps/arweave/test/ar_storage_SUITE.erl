-module(ar_storage_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("kernel/include/logger.hrl").

-compile(export_all).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,30}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
	%% Configure logging for console output
	LoggerFormatterConsole = #{
		legacy_header => false,
		single_line => true,
		chars_limit => 1024,
		depth => 26,
		template => [time," [",level,"] ",file,":",line," ",msg,"\n"]
	},
	logger:set_handler_config(default, formatter, {logger_formatter, LoggerFormatterConsole}),
	logger:set_handler_config(default, level, error),
	%% Configure logging to the logfile
	LoggerConfigDisk = #{
		file => lists:flatten("logs/"++atom_to_list(node())),
		type => wrap,
		max_no_files => 10,
		max_no_bytes => 51418800 % 10 x 5MB
	},
	logger:add_handler(disk_log, logger_disk_log_h,
					   #{config => LoggerConfigDisk,
						 level => error}),
	LoggerFormatterDisk = #{
		chars_limit => 1024,
		depth => 26,
		legacy_header => false,
		single_line => true,
		template => [time," [",level,"] ",file,":",line," ",msg,"\n"]
	},
	logger:set_handler_config(disk_log, formatter, {logger_formatter, LoggerFormatterDisk}),
	logger:set_application_level(arweave, error),
	application:set_env(arweave, data_dir, data),
	DBList = [
			  {dbcftest,
			   [{default, []}, {f1, []}, {f2, []}],
			   [{create_if_missing,true},{create_missing_column_families,true},{files,"data/filescf"}]},
			  {dbcftestnofiles,
			   [{default, []}, {f1, []}, {f2, []}],
			   [{create_if_missing,true},{create_missing_column_families,true}]},
			  {dbtest,
			   [{create_if_missing,true}, {files, "data/files"}]},
			  {dbtestnofiles,
			   [{create_if_missing,true}]}
			 ],
	%% We can't use ar_storage1:start_link due to the calling init_per_suite is wrapped by spawn
	{ok, Pid} = gen_server:start({local, ar_storage1}, ar_storage1, DBList, []),
	filelib:ensure_dir("data/filescf/"),
	file:write_file("data/filescf/k10",<<"value10">>),
	file:write_file("data/filescf/k11",<<"value11">>),
	file:write_file("data/filescf/k12",<<"value12">>),
	filelib:ensure_dir("data/files/"),
	file:write_file("data/files/k20",<<"value20">>),
	file:write_file("data/files/k21",<<"value21">>),
	file:write_file("data/files/k22",<<"value22">>),
	[{pid, Pid} | Config].

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(Config) ->
	Pid = proplists:get_value(pid, Config),
	gen_server:stop(Pid),
    ok.


%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------
groups() ->
    [
    {getTests,[sequence], [
			getValueFromDB,
			getValueFromDBCF,
			getValueMissingInDBGetValueFromFile,
			getValueMissingInDBCFGetValueFromFile,
			getValueMissingInDB,
			getValueMissingInDBCF,
			getValueUnknownDB,
			getValueUnknownDBCF
        ]},
    {putTests,[sequence], [
			putValueIntoDB,
			putValueIntoDBCF,
			putValueUnknownDB,
			putValueUnknownDBCF
        ]},
	{deleteTests,[sequence],[
			deleteValueFromDB,
			deleteValueFromDBCF,
			deleteValueFromUnknownDB,
			deleteValueFromUnknownDBCF,
			deleteRangeFromDB,
			deleteRangeFromDBCF
		]}
    ].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%% Reason = term()
%%   The reason for skipping all groups and test cases.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%--------------------------------------------------------------------

all() ->
    [
	mod_exist,
    {group, putTests},
    {group, getTests},
	{group, deleteTests}
    ].

mod_exist(_Config) ->
	{module, ar_storage1} = code:load_file(ar_storage1).
getValueFromDB(_Config) ->
	{ok, <<"value1">>} = ar_storage1:get(dbtest, <<"k1">>),
	ok.
getValueFromDBCF(_Config) ->
	{ok, <<"value2">>} = ar_storage1:get({dbcftest, f1}, <<"k2">>),
	ok.
getValueMissingInDBGetValueFromFile(_Config) ->
	{ok, <<"value20">>} = ar_storage1:get(dbtest, <<"k20">>),
	ok.
getValueMissingInDBCFGetValueFromFile(_Config) ->
	{ok, <<"value10">>} = ar_storage1:get({dbcftest, f1}, <<"k10">>),
	ok.
getValueMissingInDB(_Config) ->
	not_found = ar_storage1:get(dbtestnofiles, <<"k20">>),
	not_found = ar_storage1:get(dbtest, <<"k200">>),
	ok.
getValueMissingInDBCF(_Config) ->
	not_found = ar_storage1:get({dbcftestnofiles, f1}, <<"k10">>),
	not_found = ar_storage1:get({dbcftest, f1}, <<"k100">>),
	ok.
getValueUnknownDB(_Confif) ->
	error = ar_storage1:get(dbtestttttt, <<"k1">>),
	ok.
getValueUnknownDBCF(_Confif) ->
	error = ar_storage1:get({dbcftest, ffff}, <<"k1">>),
	ok.
putValueIntoDB(_Config) ->
	ok = ar_storage1:put(dbtest, <<"k1">>, <<"value1">>),
	% for range delete
	ok = ar_storage1:put(dbtest, <<"r1">>, <<"valueR1">>),
	ok = ar_storage1:put(dbtest, <<"r2">>, <<"valueR2">>),
	ok = ar_storage1:put(dbtest, <<"r3">>, <<"valueR3">>),
	ok.
putValueIntoDBCF(_Config) ->
	ok = ar_storage1:put({dbcftest, f1}, <<"k2">>, <<"value2">>),
	% for range delete
	ok = ar_storage1:put({dbcftest, f1}, <<"rr1">>, <<"valueRR1">>),
	ok = ar_storage1:put({dbcftest, f1}, <<"rr2">>, <<"valueRR2">>),
	ok = ar_storage1:put({dbcftest, f1}, <<"rr3">>, <<"valueRR3">>),
	ok.
putValueUnknownDB(_Confif) ->
	error = ar_storage1:put(dbtestttttt, <<"k1">>, <<"value1">>),
	ok.
putValueUnknownDBCF(_Confif) ->
	error = ar_storage1:put({dbcftest, ffff}, <<"k1">>, <<"value1">>),
	ok.
deleteValueFromDB(_Config) ->
	{ok, <<"value1">>} = ar_storage1:get(dbtest, <<"k1">>),
	ok = ar_storage1:delete(dbtest, <<"k1">>),
	not_found = ar_storage1:get(dbtest, <<"k1">>),
	ok.
deleteValueFromDBCF(_Config) ->
	{ok, <<"value2">>} = ar_storage1:get({dbcftest, f1}, <<"k2">>),
	ok = ar_storage1:delete({dbcftest,f1}, <<"k2">>),
	not_found = ar_storage1:get({dbcftest, f1}, <<"k2">>),
	ok.
deleteValueFromUnknownDB(_Config) ->
	error = ar_storage1:delete(dbtestttt,<<"k2">>),
	ok.
deleteValueFromUnknownDBCF(_Config) ->
	error = ar_storage1:delete({dbcftest,f1ffffff}, <<"k2">>),
	ok.
deleteRangeFromDB(_Config) ->
	{ok,<<"valueR1">>} = ar_storage1:get(dbtest, <<"r1">>),
	{ok,<<"valueR2">>} = ar_storage1:get(dbtest, <<"r2">>),
	{ok,<<"valueR3">>} = ar_storage1:get(dbtest, <<"r3">>),
	ok = ar_storage1:delete_range(dbtest, <<"r1">>, <<"r3">>),
	not_found = ar_storage1:get(dbtest, <<"r1">>),
	not_found = ar_storage1:get(dbtest, <<"r2">>),
	{ok,<<"valueR3">>} = ar_storage1:get(dbtest, <<"r3">>),
	ok.
deleteRangeFromDBCF(_Config) ->
	{ok,<<"valueRR1">>}= ar_storage1:get({dbcftest, f1}, <<"rr1">>),
	{ok,<<"valueRR2">>}= ar_storage1:get({dbcftest, f1}, <<"rr2">>),
	{ok,<<"valueRR3">>}= ar_storage1:get({dbcftest, f1}, <<"rr3">>),
	ok = ar_storage1:delete_range({dbcftest,f1}, <<"rr1">>, <<"rr3">>),
	not_found = ar_storage1:get({dbcftest, f1}, <<"rr1">>),
	not_found = ar_storage1:get({dbcftest, f1}, <<"rr2">>),
	{ok,<<"valueRR3">>}= ar_storage1:get({dbcftest, f1}, <<"rr3">>),
	ok.

