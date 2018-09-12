from shared import *
import time

import traceback
import ewsgi
import ework
import edb

import collections
from constants import *
from segments import Segment
import filelock
import pickle
import datetime
import shelve
import hashlib
import itertools

SCANNED_BY = 100

cell_storage_loc = '/data3/bpm/publish/cells/'
group_csv_storage_loc = '/data3/bpm/publish/groups_exterior_data/'
group_storage_loc = '/data3/bpm/publish/groups_exterior/'

publish_groups_table = 'publish_groups_exterior'

mds_publish_table = 'bpm_exterior_publish_boxoffice'
mds_publish_label = 'bpm_exterior_publish_job'



class Pid2IdIsNullException(Exception):
    pass


class MidasCanceledException(Exception):
    pass

if not os.access(cell_storage_loc, os.W_OK):
    raise PermissionError('cannot write in "%s"' % cell_storage_loc)

if not os.access(group_storage_loc, os.W_OK):
    raise PermissionError('cannot write in "%s"' % group_storage_loc)

if not os.access(group_csv_storage_loc, os.W_OK):
    raise PermissionError('cannot write in "%s"' % group_csv_storage_loc)


class Serve(ework.AsyncPipeline):

    def __init__(self):

        super().__init__()

        self.bpm_conn = edb.Database('bpm')
        self.pub_conn = edb.Database('pub')
        self.bpm_movie_conn = edb.Database('bpm_movie')

        self.fdm_conn = edb.Database('fdm')
        # self.etl_conn = dbconn.Database('etl')
        self.mds_conn = edb.Database('bestine')
        self.mds_load_conn = edb.Database('bestine_w1')

        # self.db = Database()
        self.cells_base_file = "cells_pub_std_exterior"

        self.cells = {}
        self.group = None

        self.slot_count = 0

        self.pid2moviecode = {}
        self.moviecode2mix_mode = {}
        self.moviecodeMonth2pid = {}
        # {'GF0010202':{'0123467a2018':set([('中数','租赁'),('院线','自购')])}

        self.day_monitor_task_types = [TASK_DAILY_LEASE_VS_LEASE, TASK_DAILY_MERGE_VS_ZZ]
        self.day_slots = collections.defaultdict(lambda: dict.fromkeys(self.day_monitor_task_types))
        # {(date, chain): {TaskType:TaskID, ...}}

        self.month_monitor_task_types = [TASK_MONTH_LEASE_VS_LEASE, TASK_MONTH_MON_VS_DAY, TASK_MONTH_MERGE_VS_ZZ]
        self.month_slots = collections.defaultdict(lambda: dict.fromkeys(self.month_monitor_task_types))
        # {(month, chain): {TaskType:TaskID, ...}}

        self.stopscreen_monitor_task_types = [CHAIN_RELEASE_COMPARE_CHAIN_MONTH, CHAIN_RELEASE_COMPARE_ZY_RELEASE, CHAIN_ZY_COMPARE_ZHUANZI]
        self.stopscreen_slots = collections.defaultdict(lambda: dict.fromkeys(self.stopscreen_monitor_task_types))
        # {(project, chain): {TaskType:TaskID, ...}}

        self.slot_dir = {}

        self.project_mix_mode = {}
        # {(moviecode, chain): {TaskType:TaskID, ...}}

        self.pid_id_map = {}

        return

    @ework.worker(sleep=3)
    async def monitor_project_info(self):

        while (True):
            try:
                await self.wait()
                imax_way = {
                    IMAX_CHAIN: {('chain', DEVICE_LEASED), ('chain', DEVICE_OWNED)},
                    IMAX_ZHONGYING: {('zy', DEVICE_LEASED), ('zy', DEVICE_OWNED)},
                    IMAX_DEFAULT: {('zy', DEVICE_LEASED), ('chain', DEVICE_OWNED)}
                }
                pid_moivecode = self.fdm_conn('''
                                select tm.pid as pid, lower(tm.code) as moviecode, tm.edition as edition,
                                 tm.zone as zone,tp.ptype as ptype, tp.pclass as pclass, tp.pfamily as pfamily
                                from fdm_moviecode tm left join fdm_project tp
                                on tm.pid = tp.pid where tp.ptype != 99
                                ''')

                imax_option = dict(self.bpm_movie_conn.tuple(
                    '''select t1.pid as pid, t1.settlement_way_imax as imax_way from
                        bpm_options t1 left join (select pid, max(id) as id from bpm_options GROUP by pid) t2
                        on t1.id=t2.id where  t1.settlement_way_imax in (%s,%s)''', (IMAX_CHAIN, IMAX_ZHONGYING)))

                returned_project = self.bpm_movie_conn(
                    '''select pid, start_month, end_month from bpm_project_returned''')

                returned_project_dict = {r['pid']: (r['start_month'], r['end_month']) for r in returned_project}

                pid2moviecode = {}
                moviecode2mix_mode = {}
                moviecodeMonth2pid = {}
                for pc in pid_moivecode:
                    month_tuple = returned_project_dict.get(pc['pid'], (None, None))
                    if pc['edition'] in (IMAXJP, IMAX2D, IMAX3D):  # 如果排次号的版本为imax,查找指定的组合方法，为指定选择默认
                        im_v = imax_option.get(pc['pid'], IMAX_DEFAULT)
                    else:
                        im_v = IMAX_DEFAULT

                    pid2moviecode.setdefault(pc['pid'], {})

                    pid2moviecode[pc['pid']][pc['moviecode']] = (month_tuple, imax_way.get(im_v))
                    # back_project_info.append((month_tuple[0], month_tuple[1],pc['moviecode'],pc['pid'],pc['zone']))

                    moviecode2mix_mode.setdefault(pc['moviecode'], Segment())
                    moviecode2mix_mode[pc['moviecode']][month_tuple[0]:month_tuple[1]] = imax_way.get(im_v)

                    moviecodeMonth2pid.setdefault(pc['moviecode'], Segment())
                    # 复映的pid, 缺少排次号，但是该排次号在复映期间有票房
                    moviecodeMonth2pid[pc['moviecode']][month_tuple[0]:month_tuple[1]] = {
                        'pid': pc['pid'],
                        'ptype': pc['ptype'],
                        'pclass': pc['pclass'],
                        'pfamily': pc['pfamily']
                    }

                self.pid_id_map = dict(self.fdm_conn.tuple('select pid, id from fdm_project_id'))

                changed = bool(self.pid2moviecode != pid2moviecode)

                self.moviecode2mix_mode = moviecode2mix_mode

                if changed:
                    self.pid2moviecode = pid2moviecode
                    self.generate_cells.touch()

                changed_pid = False
                if len(self.moviecodeMonth2pid) != len(moviecodeMonth2pid):
                    changed_pid = True
                else:

                    t1 = sorted(list((mc, seg.segs) for mc, seg in moviecodeMonth2pid.items()))
                    t2 = sorted(list((mc, seg.segs) for mc, seg in self.moviecodeMonth2pid.items()))
                    if t1 != t2:
                        changed_pid = True

                if changed_pid:
                    self.moviecodeMonth2pid = moviecodeMonth2pid
                    self.generate_groups.touch()
            except Exception as e:
                print('---------->')
                traceback.print_exc()

    @ework.worker(sleep=3)
    async def monitor_day_task(self):
        tasks = self.get_latest_task(self.day_monitor_task_types)
        for _ in tasks:
            _['period'] = _['period'].strftime('%Y%m%d')
            if _['period'] < '20171201':
                continue
            self.day_slots[(int(_['unit']), int(_['period']))][_['type']] = _['tid']
            self.set_scanned(_['tid'])

        while (True):
            try:
                await self.wait()
                changed = False
                tasks = self.get_latest_task(self.day_monitor_task_types, True)
                for _ in tasks:
                    _['period'] = _['period'].strftime('%Y%m%d')
                    if self.day_slots[(int(_['unit']), int(_['period']))][_['type']] != _['tid']:
                        changed = True
                        self.day_slots[(int(_['unit']), int(_['period']))][_['type']] = _['tid']
                        self.set_scanned(_['tid'])

                if changed:
                    self.generate_cells.touch()
            except Exception as e:
                print('---------->')
                traceback.print_exc()

    @ework.worker(sleep=3)
    async def monitor_month_task(self):

        tasks = self.get_latest_task(self.month_monitor_task_types)

        for _ in tasks:
            _['period'] = _['period'].strftime('%Y%m%d')
            self.month_slots[(int(_['unit']), int(_['period'][:6]))][_['type']] = _['tid']
            self.set_scanned(_['tid'])

        while (True):
            try:
                await self.wait()
                changed = False
                tasks = self.get_latest_task(self.month_monitor_task_types, True)
                for _ in tasks:
                    _['period'] = _['period'].strftime('%Y%m%d')
                    if self.month_slots[(int(_['unit']), int(_['period'][:6]))][_['type']] != _['tid']:
                        changed = True
                        self.month_slots[(int(_['unit']), int(_['period'][:6]))][_['type']] = _['tid']
                        self.set_scanned(_['tid'])

                if changed:
                    self.generate_cells.touch()
            except Exception as e:
                print('---------->', str(e))

    @ework.worker(sleep=3)
    async def monitor_stopscreen_task(self):
        tasks = self.get_latest_task(self.stopscreen_monitor_task_types)
        wanpian_pids = self.bpm_movie_conn('''select pid from bpm_wanpian_project''')
        wanpian_pids_sets = set([p['pid'] for p in wanpian_pids])

        for _ in tasks:
            if _['target'] in wanpian_pids_sets:  # 过滤完片归档的项目
                continue
            self.stopscreen_slots[(int(_['unit']), _['target'])][_['type']] = _['tid']
            self.set_scanned(_['tid'])

        while (True):
            try:
                await self.wait()
                changed = False

                tasks = self.get_latest_task(self.stopscreen_monitor_task_types, True)
                wanpian_pids = self.bpm_movie_conn('''select pid from bpm_wanpian_project''')
                wanpian_pids_sets = set([p['pid'] for p in wanpian_pids])
                for _ in tasks:
                    if _['target'] in wanpian_pids_sets:  # 过滤完片归档的项目
                        continue
                    if self.stopscreen_slots[(int(_['unit']), _['target'])][_['type']] != _['tid']:
                        changed = True
                        self.stopscreen_slots[(int(_['unit']), _['target'])][_['type']] = _['tid']
                        self.set_scanned(_['tid'])

                if changed:
                    self.generate_cells.touch()
            except Exception as e:
                print('---------->')
                traceback.print_exc()



    @ework.worker()
    async def load_groups(self):

        while True:
            try:
                await self.wait()
                # 如果最近的一次 groups 导入状态为 invalid 状态，说明 midas 出现过未知错误
                latest_group = self.pub_conn(f'select * from {publish_groups_table} order by id desc limit 1')[0]
                if latest_group['status'] == STATUS_VALID:
                    self.do_load(latest_group['groups'])
                    self.clean_groups()
                else:
                    print('存在失败的 group')
            except Exception as e:
                print('---------->')
                traceback.print_exc()


    def clean_groups(self):
        files = []
        for f in os.listdir(group_storage_loc):
            if not f.endswith('json'):
                continue
            files.append({
                'name': os.path.join(group_storage_loc, f),
                'ctime': os.path.getctime(os.path.join(group_storage_loc, f)),
                'md5': f.split('.')[0]
            })

        sorted_files = sorted(files, key=lambda e: e['ctime'], reverse=True)

        for f in sorted_files[5:]:
            if f['name'].endswith('init.json'):
                continue
            self.pub_conn(f'delete from {publish_groups_table} where groups = %s', f['md5'])
            os.remove(f['name'])

        for f in os.listdir(group_csv_storage_loc):
            os.remove(os.path.join(group_csv_storage_loc, f))

    @ework.worker(precond=[monitor_day_task, monitor_month_task, monitor_stopscreen_task, monitor_project_info])
    async def generate_cells(self):
        while (True):
            try:
                await self.wait()
                self.slot_count = 0
                day_slots = list(self.parse_slots(self.day_slots, 'day'))
                month_slots = list(self.parse_slots(self.month_slots, 'mon'))
                stopscreen_slots = list(self.parse_slots(self.stopscreen_slots, 'scn'))

                # create new cell
                self.construct_cell_base(itertools.chain(day_slots, month_slots, stopscreen_slots))

                # generate
                stopscreens = {}
                for (chain, pid), tasks, slot_task_key in stopscreen_slots:
                    for moviecode, ((st, ed), mix_mode) in self.pid2moviecode[pid].items():
                        stopscreens.setdefault((moviecode, int(chain)), Segment())
                        stopscreens[(moviecode, int(chain))][st:ed] = True

                monthreported = [(month, chain) for (chain, month), tasks, slot_task_key in month_slots]
                monthreported = set(monthreported)

                pid_ignore = [_r['pid'] for _r in self.bpm_movie_conn('select pid from bpm_project_ignore')]
                with shelve.open(self.cells_base_file) as cells_base:
                    cells = []
                    for slot, tasks, slot_task_key in itertools.chain(day_slots, month_slots, stopscreen_slots):
                        if slot[1] in pid_ignore:
                            continue
                        for cell in cells_base[slot_task_key]:

                            # DAY MONTH STOPSCREEN
                            #  y
                            #  y   y
                            #  y   y     y

                            # 补齐缺失moviecode组合模式为默认组合
                            self.repair_moviecode_mode(cell)

                            if cell['data_level'] == 'day' and (cell['showmonth'], cell['chain']) in monthreported:
                                continue

                            if cell['data_level'] != 'scn':
                                mc_segment = stopscreens.get((cell['moviecode'], cell['chain']), Segment())
                                if mc_segment[cell['showmonth']]:
                                    continue

                            if (cell['source'], cell['device']) not in self.moviecode2mix_mode[cell['moviecode']][cell['showmonth']]:
                                continue

                            cells.append(cell)
                    self.cells = cells
                self.generate_groups.touch()

            except MoviecodeError:
                pass
            except CinemaError:
                pass
            except Exception as e:
                print('---------->')
                traceback.print_exc()


    @ework.worker()
    async def generate_groups(self):
        while (True):
            try:
                await self.wait()
                cells_info = self.cells_info_scan()
                self.group = self.construct_groups(cells_info)
                self.filter_cells_zone()
                self.load_groups.touch()
            except Exception as e:
                print(str(e))

    def get_latest_task(self, monitor_task_types, not_scanned=False):
        if not_scanned:
            tasks = self.bpm_conn(
                f'''
                select `target`, `unit`, `period`,  `type`, `tid`
                from
                (
                    select `target`, `unit`, `period`,  `type`, max(`id`) as `tid` 
                    from `bpm_statistics_tasks` where `type` in ({','.join(map(str, monitor_task_types))}) 
                    and `status` = %s  and `state` = %s 
                    group by `target`, `unit`, `period`, `type`
                ) as `t` 
                left join `bpm_scanned_tasks` as `st` 
                on `t`.`tid` = `st`.`task_id` and `st`.`scanned_by` = %s
                where `st`.`task_id` is null
                ''',
                (STATUS_NORMAL, STATE_STABLED, SCANNED_BY)
            )

        else:
            tasks = self.bpm_conn(
                f'''select `target`, `unit`, `period`,  `type`, max(`id`) as `tid` from `bpm_statistics_tasks` 
                    where `type` in ({','.join(map(str, monitor_task_types))}) and `status` = %s and `state` = %s
                    group by `target`, `unit`, `period`, `type` ''',
                (STATUS_NORMAL, STATE_STABLED)
            )
        return tasks

    def set_scanned(self, task_id):
        self.bpm_conn('insert ignore into bpm_scanned_tasks value (null, %s, %s)', (task_id, SCANNED_BY))

    def get_moviecode_id(self, moviecode):
        """
        :param mc_in_cell: set
        :return:
        """
        r = self.bpm_conn('select `id`, `code` from `uni_moviecodes` where `code` = %s', moviecode)
        if r:
            return r[0]['id']
        else:
            conn = edb.Database('w1_etl')
            try:
                moviecode_id = conn('select `id`, `code` from `obj_moviecodes` where `code` = %s', moviecode)

                if moviecode_id:
                    return moviecode_id[0]['id']
                else:
                    return conn('insert into `obj_moviecodes` (`id`, `code`) value (null, %s)', moviecode)

            except Exception as e:
                print(str(e))
            finally:
                conn.tvar.conn.close()

        raise MoviecodeError

    def repair_moviecode_mode(self, cell):
        # 1，排次号错误，添加默认模式
        if cell['moviecode'] not in self.moviecode2mix_mode:  # 未知影片赋予默认值
            self.moviecode2mix_mode.setdefault(cell['moviecode'], Segment())
            self.moviecode2mix_mode[cell['moviecode']][:] = set([('zy', DEVICE_LEASED), ('chain', DEVICE_OWNED)])

        # 2，复映排次号缺失月份对应模式，添加默认模式
        if not self.moviecode2mix_mode[cell['moviecode']][cell['showmonth']]:
            self.moviecode2mix_mode[cell['moviecode']][cell['showmonth']:] = set([('zy', DEVICE_LEASED), ('chain', DEVICE_OWNED)])

    def parse_slots(self, slot, data_level):
        for k, v in slot.items():

            if None in list(v.values()):
                continue

            slot_task_key = '&'.join(['%s=%s' % (k, v) for k, v in sorted(v.items())])
            self.slot_count += 1
            yield k, v, '%s@%s' % (data_level, slot_task_key)

        return ''

    def construct_cell_base(self, slots):
        all_cm_city = dict(self.bpm_conn.tuple('select `code`, `city` from `uni_cinemas`'))
        with shelve.open(self.cells_base_file) as cells_base:

            slot_current = 0
            for slot, tasks, slot_task_key in slots:
                slot_current += 1
                print('slots progress: %s / %s' % (slot_current, self.slot_count))
                slot_dir = get_slot_key_dir(slot_task_key, slot[0], cell_storage_loc)
                self.slot_dir[slot_task_key] = slot_dir

                slot_task_key_dir = os.path.join(slot_dir, slot_task_key)
                query_slice = {
                    'dim_cell_row': ' `book`, `showdate`, `showmonth`, `moviecode`, `cinema`, `chain`, `province`, `device`, sum(`shows`), sum(`audience`), sum(`revenue`), count(distinct(`hall`)) as hall_cnt ',
                    'group_cell_row': ' `book`, `showdate`, `showmonth`, `moviecode`, `cinema`, `chain`, `province`, `device` ',

                    'day': {
                        'dim': ' `showdate`, `showmonth`, `moviecode`, `chain`, `book`, `device` ',
                        'dim_cond': '`chain` = %s and `showdate` = %s and `book` in (1, 3)' % slot,
                        'row_cond': ' `moviecode` = %s and `showdate` = %s and `showmonth` = %s and `chain` = %s and `book` = %s and `device` = %s '
                    },
                    'mon': {
                        'dim': ' `showmonth`, `moviecode`, `chain`, `book`, `device`',
                        'dim_cond': '`chain` = %s and `showmonth` = %s and `book` in (2, 6)' % slot,
                        'row_cond': ' `moviecode` = %s and `chain` = %s and `showmonth` =%s and `book` = %s and `device` = %s '
                    },
                    'scn': {
                        'dim': ' `showmonth`, `moviecode`, `chain`, `book`, `device` ',
                        'dim_cond': f'''`chain` = {slot[0]} and `moviecode` in ('{ "','".join(self.pid2moviecode[slot[1]].keys()) if isinstance(slot[1], str) else ''}') and `book` in (4, 7, 8 )''',
                        'row_cond': ' `moviecode` = %s and `chain` = %s and `showmonth` =%s and `book` = %s and `device` = %s '
                    }
                }

                if slot_task_key not in cells_base:
                    try:
                        os.makedirs(slot_task_key_dir)
                    except FileExistsError:
                        pass

                    with filelock.FileLock(os.path.join(slot_dir, '%s.lock' % slot_task_key)):

                        try:
                            with open(os.path.join(slot_task_key_dir, "cell.info")) as ifp:
                                cells_info = json.load(ifp)
                        except FileNotFoundError:
                            cells_info = []

                            data_lv = slot_task_key[:3]
                            task_dimension = self.mds_conn(
                                f'''select {query_slice[data_lv]['dim']}
                                  from `stt_boxoffice` 
                                  where {query_slice[data_lv]['dim_cond']} 
                                  group by {query_slice[data_lv]['dim']}
                                  '''
                            )

                            cell_current, cell_all = 0, len(task_dimension)
                            for dim in task_dimension:
                                cell_current += 1
                                print('slots progress: %s / %s, cells progress: %s / %s' % (
                                    slot_current, self.slot_count, cell_current, cell_all))
                                book_source = get_source(dim['book'])
                                if book_source is None:
                                    continue

                                # cell_name 的组成与 dim 是一致的，可以认为根据 cell_name 即可重新获取到 cell
                                if data_lv == 'day':
                                    args = (
                                        dim['moviecode'], dim['showdate'], dim['showmonth'], dim['chain'], dim['book'],
                                        dim['device']
                                    )
                                    cell_name = f"{book_source}_{dim['book']}_{dim['showdate']}_{dim['chain']}_{dim['moviecode']}_{dim['device']}"
                                else:
                                    args = (
                                        dim['moviecode'], dim['chain'], dim['showmonth'], dim['book'], dim['device']
                                    )
                                    cell_name = f"{book_source}_{dim['book']}_{dim['showmonth']}_{dim['chain']}_{dim['moviecode']}_{dim['device']}"

                                cell_info = {
                                    'cell_name': cell_name,
                                    'slot_key': slot_task_key,
                                    'slot_dir': slot_task_key_dir.replace(cell_storage_loc, ''),
                                    'book': dim['book'], 'source': book_source, 'data_level': data_lv,
                                    'showmonth': dim['showmonth'],
                                    'chain': dim['chain'], 'moviecode': dim['moviecode'], 'device': dim['device'],
                                }

                                cell_csv_pth = os.path.join(slot_task_key_dir, cell_name + '.csv')
                                cell_rows = self.mds_conn(
                                    f'''select {query_slice['dim_cell_row']}
                                    from stt_boxoffice
                                    where {query_slice[data_lv]['row_cond']}
                                    group by {query_slice['group_cell_row']}
                                    ''', args
                                )

                                showdates = set()
                                cinemas = set()
                                chains = set()
                                shengs = set()
                                cnt = 0

                                moviecode_id = self.get_moviecode_id(cell_rows[0]['moviecode'])

                                with open(cell_csv_pth, 'w') as cell_fp:
                                    csv_writer = csv.writer(cell_fp)
                                    result = []
                                    for r in cell_rows:
                                        showdates.add(int(r['showdate']))
                                        cinemas.add(int(r['cinema']))
                                        chains.add(int(r['chain']))
                                        shengs.add(int(r['province']))
                                        cnt += 1
                                        r['moviecode_id'] = moviecode_id
                                        try:
                                            r['city'] = int(all_cm_city[r['cinema']])
                                        except:
                                            r['city'] = 0
                                        result.append(tuple(r.values()))

                                    csv_writer.writerows(result)

                                cinemas = sorted(list(cinemas))
                                firstdate = min(showdates)
                                lastdate = max(showdates)
                                showdates = sorted(list(showdates))
                                shengs = sorted(list(shengs))
                                ex_info = {
                                    'firstdate': firstdate,
                                    'lastdate': lastdate,
                                    'showdates': showdates,
                                    'cinemas': cinemas,
                                    'shengs': shengs,
                                    'rows': cnt,
                                }
                                cell_info.update(ex_info)
                                with open(os.path.join(slot_task_key_dir, '%s.info' % cell_name), 'wb') as fp:
                                    pickle.dump(cell_info, fp)

                                cell_info.pop('showdates')
                                cell_info.pop('cinemas')
                                cell_info.pop('shengs')

                                cells_info.append(cell_info)

                            with open(os.path.join(slot_task_key_dir, "cell.info"), 'w') as ifp:
                                json.dump(cells_info, ifp)

                        cells_base[slot_task_key] = cells_info

    def cells_info_scan(self):
        cells_info = []

        for cell in self.cells:
            with filelock.FileLock(os.path.join(self.slot_dir[cell['slot_key']], '%s.lock' % cell['slot_key'])):
                cell_info_path = os.path.join(cell_storage_loc, cell['slot_dir'], '%s.info' % cell['cell_name'])

                with open(cell_info_path, 'rb') as cfp:
                    cell_info = pickle.load(cfp)
                    cells_info.append(cell_info)

        return cells_info

    def construct_groups(self, cells_info):
        """
        :param cells_info:
        [
            {
                <cell_info>,
            },
        ]
        :return:
        [
            (slot_key, cell_name, ((str<pinfo_property>, int<pinfo_value>), ...), ((int<cinema_code>, int<city_code>), ...))
        ]
        """
        group = set()

        for cell_info in cells_info:
            pinfo = tuple(
                sorted(
                    [(prop, val) for prop, val in
                     self.get_cell_pinfo(cell_info['moviecode'], cell_info['showmonth']).items()]
                )
            )

            group.add((
                cell_info['slot_key'],
                cell_info['cell_name'],
                (
                    ('path', os.path.join(cell_storage_loc, cell_info['slot_dir'], '%s.csv' % cell_info['cell_name'])),
                    ('book', cell_info['book'],),
                    ('source', cell_info['source'],),
                    ('data_level', cell_info['data_level'],),
                    ('firstdate', cell_info['firstdate'],),
                    ('showmonth', cell_info['showmonth'],),
                    ('chain', cell_info['chain'],),
                    ('moviecode', cell_info['moviecode'],),
                    ('device', cell_info['device'],)
                ),
                pinfo
            ))

        return group

    def filter_cells_zone(self):
        mc_zone = dict(self.fdm_conn.tuple('select code, zone from fdm_moviecode'))
        ch_zone = dict(self.bpm_conn.tuple('select id, zone from uni_chains where status = 0'))
        new_group = set()
        for g in self.group:
            g_ch = dict(g[2])['chain']
            g_mc = dict(g[2])['moviecode']

            if g_mc in mc_zone:
                if mc_zone[g_mc] == ch_zone[g_ch] or mc_zone[g_mc] in (0, 3):
                    new_group.add(g)
            else:
                new_group.add(g)

        self.group = new_group


    def get_cell_pinfo(self, moviecode, showmonth):
        if moviecode not in self.moviecodeMonth2pid:
            return {'pid': 0, 'id': 0, 'ptype': 0, 'pclass': 0, 'pfamily': 0}
        if not self.moviecodeMonth2pid[moviecode][showmonth]:
            return {'pid': 0, 'id': 0, 'ptype': 0, 'pclass': 0, 'pfamily': 0}

        back_dict = self.moviecodeMonth2pid[moviecode][showmonth].copy()
        if not self.pid_id_map.get(back_dict['pid']):
            res = self.fdm_conn('select pid_id, id from fdm_project_id where pid_id=%s', back_dict['pid'])
            if res:
                pid2id = res[0]['id']
            else:
                raise Pid2IdIsNullException
        else:
            pid2id = self.pid_id_map.get(back_dict['pid'])

        back_dict['id'] = pid2id
        return back_dict

    def do_load(self, g_base_md5):
        g_base = load_group_from_json(g_base_md5,group_storage_loc)
        if g_base_md5 == 'init':
            going_groups = diff_group(g_base, self.group)
            going_groups_succ, going_groups_fail, going_groups_all = 0, 0, len(going_groups)

            for mc, cells in going_groups.items():

                g_dest = g_base | cells['to_add']
                g_dest = tuple(g_dest - cells['to_del'])
                g_dest_md5 = hashlib.md5(json.dumps(g_dest).encode()).hexdigest()
                g_id = self.pub_conn(
                    f'insert ignore into {publish_groups_table} value (null, %s, %s, %s)',
                    ( g_dest_md5, STATUS_INVALID, datetime.datetime.now())
                )

                if not g_id:
                    continue

                with open(os.path.join(group_storage_loc, g_dest_md5 + '.json'), 'w') as gfp:
                    json.dump(g_dest, gfp)

                g_load = GroupLoadFile(g_id, group_csv_storage_loc, mds_publish_label)

                group_data_combination_exterior(cells['to_add'], cells['to_del'], g_load)

                if self.do_upload(g_load):
                    going_groups_succ += 1
                    g_base = set(g_dest)
                    self.pub_conn(f'update {publish_groups_table} set status = %s where id = %s',
                                  (STATUS_VALID, g_load.identify))
                    print('[进度]  成功:%d/失败:%d/总数:%d' % (going_groups_succ, going_groups_fail, going_groups_all))
                else:
                    going_groups_fail += 1
                    g_base = set(g_dest)
                    self.pub_conn(f'update {publish_groups_table} set status = %s where id = %s',
                                  (STATUS_UPLOAD_FAILED, g_load.identify))
                    print('[进度]  成功:%d/失败:%d/总数:%d' % (going_groups_succ, going_groups_fail, going_groups_all))
        else:
            to_del = g_base - self.group
            to_add = self.group - g_base

            g_dest_md5 = hashlib.md5(json.dumps(tuple(self.group)).encode()).hexdigest()
            g_id = self.pub_conn(
                f'insert ignore into {publish_groups_table} value (null, %s, %s, %s)',
                (g_dest_md5, STATUS_INVALID, datetime.datetime.now())
            )

            if not g_id:
                return

            with open(os.path.join(group_storage_loc, g_dest_md5 + '.json'), 'w') as gfp:
                json.dump(tuple(self.group), gfp)

            g_load = GroupLoadFile(g_id, group_csv_storage_loc, mds_publish_label)

            group_data_combination_exterior(to_add, to_del, g_load)

            if self.do_upload(g_load):
                self.pub_conn(f'update {publish_groups_table} set status = %s where id = %s',
                              (STATUS_VALID, g_load.identify))
                print('[进度]  成功:1/失败:0/总数:1')
            else:
                self.pub_conn(f'update {publish_groups_table} set status = %s where id = %s',
                              (STATUS_UPLOAD_FAILED, g_load.identify))
                print('[进度]  成功:0/失败:1/总数:1')

    def do_upload(self, g_load):
        try:
            r = self.mds_load_conn.upload_csv(mds_publish_table, g_load.label, g_load.filename)
            print(r)
            while True:
                ups = self.mds_load_conn.upload_status(g_load.label)
                if not ups:
                    time.sleep(1)
                    continue

                if ups['State'] == 'FINISHED':
                    return True

                if ups['State'] == 'CANCELLED':
                    return False

                time.sleep(0.5)
        except Exception as err:
            print(err)
            raise


class App(ewsgi.WsgiServer):

    def __init__(self):
        super().__init__()

        self.p = Serve()

        return

    def url__status(self):
        r = ' / '.join(["%s %s" % (n, t.count) for n, t in self.p._works.items()])

        return r + '\n'

    def url__generate_cells(self):
        self.p.generate_cells.touch()


if __name__.startswith('uwsgi_file_'):
    application = App()
    application.p.run()

    # uwsgi --asyncio 3 --http-socket :9090 --greenlet --wsgi-file example.py
    # uwsgi --asyncio 3 --http-socket :9090 --wsgi-file example.py
