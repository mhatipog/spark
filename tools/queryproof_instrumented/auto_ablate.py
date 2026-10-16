"""Automatic, blinded-to-rule-name ablation of top AST candidates.

Candidates come solely from pre/post AST structural changes. Expected answers are
used only after selection to evaluate interventions. Plan changes are heuristic,
not proofs of rule responsibility. Unknown and mandatory rules stay INCONCLUSIVE.
"""
import json, re, sys, tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from queryproof_spark_probe import cases,run
from queryproof_rule_ablation import setup

def normalized(text):
    return re.sub(r'#\d+L?', '#ID', text)

def main():
    triage=json.loads(Path('artifacts/triage.json').read_text())
    evidence=json.loads(Path('artifacts/queryproof_spark_results.json').read_text())
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof-Auto-Ablation')
        .config('spark.ui.enabled','false').config('spark.sql.adaptive.enabled','false')
        .config('spark.sql.shuffle.partitions','5')
        .config('spark.sql.autoBroadcastJoinThreshold','-1').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    results={'spark_version':spark.version,'cases':{},
        'warning':'Ablation restoration is intervention evidence, not formal semantic proof.'}
    try:
        with tempfile.TemporaryDirectory() as td:
            for name,c in cases.items():
                expected=c['expected']
                baseline=evidence['cases'][name]
                observed=baseline['actual']['rows']
                control=baseline['control']['rows']
                if observed==expected or control!=expected:
                    results['cases'][name]={'status':'SKIPPED_NO_CONFIRMED_WRONG_ANSWER'}
                    continue
                ranked=triage['ranked_candidates'].get(name,[])[:4]
                item={'status':'NOT_LOCALIZED','candidates':[]}
                results['cases'][name]=item
                for idx,candidate in enumerate(ranked):
                    rule=candidate['rule']
                    session=spark.newSession()
                    session.conf.set('spark.sql.optimizer.excludedRules',rule)
                    dest=str(Path(td)/re.sub('[^a-z0-9]+','_',name.lower())/str(idx))
                    try:
                        setup(session,dest)
                        actual=run(session,c['sql'])
                        same_plan=(normalized(actual['optimized'])==
                                   normalized(baseline['actual']['optimized']))
                        correct=actual['rows']==expected
                        if same_plan:
                            status='NO_OBSERVABLE_PLAN_CHANGE'
                        elif correct:
                            status='RESTORED_WITH_CHANGED_PLAN'
                        else:
                            status='WRONG_RESULT_PERSISTS'
                        item['candidates'].append({'rule':rule,'score':candidate['score'],
                            'features':candidate['features'],'status':status,
                            'plan_changed':not same_plan,'restored_expected_answer':correct,
                            'actual_rows':actual['rows']})
                        print('QUERYPROOF_ABLATION',name,idx,status,rule,flush=True)
                        if status=='RESTORED_WITH_CHANGED_PLAN':
                            item['status']='SUSPECT_LOCALIZED_BY_INTERVENTION'
                        if status=='NO_OBSERVABLE_PLAN_CHANGE':
                            # Testing-only fallback for mandatory rules that Spark ignores in
                            # spark.sql.optimizer.excludedRules. Never run outside a disposable JVM.
                            second=spark.newSession()
                            forced_dest=str(Path(dest)/'forced')
                            try:
                                setup(second,forced_dest)
                                system=spark._jvm.java.lang.System
                                system.setProperty('queryproof.force.skipRule',rule)
                                system.setProperty('queryproof.force.skipCount','0')
                                system.setProperty('queryproof.force.enabled','true')
                                try:
                                    forced=run(second,c['sql'])
                                    skips=int(system.getProperty('queryproof.force.skipCount','0'))
                                finally:
                                    system.clearProperty('queryproof.force.enabled')
                                    system.clearProperty('queryproof.force.skipRule')
                                    system.clearProperty('queryproof.force.skipCount')
                                fplan=(normalized(forced['optimized']) !=
                                       normalized(baseline['actual']['optimized']))
                                fcorrect=forced['rows']==expected
                                if skips==0:
                                    fstatus='SKIP_NOT_APPLIED'
                                elif not fplan:
                                    fstatus='NO_OBSERVABLE_PLAN_CHANGE'
                                elif fcorrect:
                                    fstatus='RESTORED_WITH_FORCED_ABLATION'
                                    item['status']='SUSPECT_LOCALIZED_BY_FORCED_INTERVENTION'
                                else:
                                    fstatus='WRONG_RESULT_PERSISTS'
                                item['candidates'][-1]['forced_ablation']={
                                    'status':fstatus,'skip_invocations':skips,
                                    'plan_changed':fplan,'restored_expected_answer':fcorrect,
                                    'actual_rows':forced['rows']}
                                print('QUERYPROOF_FORCED',name,idx,fstatus,rule,flush=True)
                            except Exception as ex:
                                # Invalid/unexecutable intermediate plans are not localization.
                                item['candidates'][-1]['forced_ablation']={
                                    'status':'INCONCLUSIVE_ERROR','error':repr(ex)}
                                print('QUERYPROOF_FORCED',name,idx,'INCONCLUSIVE_ERROR',repr(ex),flush=True)
                            finally:
                                system=spark._jvm.java.lang.System
                                system.clearProperty('queryproof.force.enabled')
                                system.clearProperty('queryproof.force.skipRule')
                                system.clearProperty('queryproof.force.skipCount')
                    except Exception as ex:
                        item['candidates'].append({'rule':rule,'status':'INCONCLUSIVE_ERROR',
                                                   'error':repr(ex)})
                        print('QUERYPROOF_ABLATION',name,idx,'INCONCLUSIVE_ERROR',repr(ex),flush=True)
    finally:
        spark.stop()
    Path('artifacts/automatic_ablation.json').write_text(json.dumps(results,indent=2))
    return 0
if __name__=='__main__':sys.exit(main())
