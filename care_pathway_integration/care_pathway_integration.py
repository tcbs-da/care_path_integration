import pandas as pd
# from zipfile import ZipFile

import warnings
warnings.filterwarnings("ignore","DeprecationWarning")


class care_pathway(object):

    def __init__(self,df):

        self.df = df


    def later_to_first(self, ori_first, add_later):
        """我们发现被重组到初诊中的‘伪复诊’可能与其之前的初诊记录拥有相同的“治疗计划” 、“诊断名称”成为了伪初诊，我们将它们筛选出来并重新放回复诊数据中，因为这部分数据很可能是医生复制了其初诊计划"""
        print("We have original first dataset in size {}, later dataset in size {}".format(len(ori_first),
                                                                                           len(add_later)))
        id_l = set(add_later[['关联键', '证件号（id）', '诊断名称', '科室']].value_counts().index)
        idx_l = set(add_later.index)
        keep = set()
        rm = set()
        # 从需要加入的id_l中遍历原初诊数据集，查询其中是否有诊断名称、治疗计划相同的rows
        list_idx = []
        for i in id_l:  # 遍历每一个组合key
            ori_sub = ori_first[(ori_first['关联键'].values == i[0]) & (ori_first['证件号（id）'].values == i[1]) & (
                    ori_first['诊断名称'].values == i[2]) & (ori_first['科室'].values == i[3])]
            ori_checklist = set(ori_sub['治疗计划'].unique())
            add_sub = add_later[(add_later['关联键'].values == i[0]) & (add_later['证件号（id）'].values == i[1]) & (
                    add_later['诊断名称'].values == i[2]) & (add_later['科室'].values == i[3])]
            add_checklist = set(add_sub['治疗计划'].unique())
            overlap = ori_checklist & add_checklist

            if ori_sub.empty:
                rm = rm | set(list(add_sub.index))

            elif len(overlap) > 0:  # 如果有重叠的项，则将复诊留在复诊集
                keepinlater = list(add_sub[add_sub['治疗计划'].isin(overlap)].index)
                keep = keep | set(keepinlater)

            elif 'Unknown' in add_checklist:  # 治疗计划 == unknown， 保留在复诊集
                keepinlater = list(add_sub[add_sub['治疗计划'].values == 'Unknown'].index)
                keep = keep | set(keepinlater)

        print("Here are/is {} laters back to later set".format(len(keep)))

        idx_l = idx_l - keep - rm
        final_add = add_later.loc[idx_l, :]
        final_back = add_later.loc[keep - rm, :]
        print(len(idx_l) + len(keep))
        print("Here are/is {} rows' later data added to first".format(final_add.shape[0]))

        return final_add, final_back

    def refill_treat(self, first, later):
        """将复诊中的Unknown用初诊中的具体治疗计划替代，因为我们在后面的初复诊配对流程中要用到“治疗计划”而保证整料路径的精确性
        first: 带具体治疗计划的初诊数据
        later：无具体治疗计划的复诊数据
        """
        # 优先级1： 日期比自身早
        combined_laterid = later[['关联键', '证件号（id）', '科室', '诊断名称']].drop_duplicates(
            keep='first').values  # 所有患者复诊的key
        for i in combined_laterid:
            # print("\n*****该复诊为*****\n",later[(later['关联键'].values == i[0])&(later['证件号（id）'].values == i[1])&(later['科室'].values == i[2])&(later['诊断名称'].values == i[3])])
            firsts = first[
                (first['关联键'].values == i[0]) & (first['证件号（id）'].values == i[1]) & (first['科室'].values == i[2]) & (
                        first['诊断名称'].values == i[3])]
            if len(firsts) > 0:
                uni_treat = firsts[['关联键', '证件号（id）', '科室', '诊断名称', '治疗计划', 'date']].drop_duplicates(
                    keep="first")  # 正常情况下 只有一种治疗计划，但也可能中途变更产生新的病程
                # print("\n*****此复诊对应的初诊病例有如下*****\n",uni_treat)

                later_date = later[
                    (later['关联键'].values == i[0]) & (later['证件号（id）'].values == i[1]) & (
                            later['科室'].values == i[2]) & (
                            later['诊断名称'].values == i[3])]['date'].unique()
                # 优先级2：识别一共有几个比该次复诊早的初诊，若有两个及两个以上，则取最近一次的
                for x in later_date:
                    choice = uni_treat[uni_treat['date'].values <= x].sort_values(by='date', ascending=True)
                    # print("\n*****比该复诊早的初诊如下*****\n",choice)
                    if len(choice) > 0:
                        real_treat = choice['治疗计划'].values[-1]
                        later.loc[(later['date'].values == x) & (later['关联键'].values == i[0]) & (
                                later['证件号（id）'].values == i[1]) & (later['科室'].values == i[2]) & (
                                          later['诊断名称'].values == i[3]), '治疗计划'] = real_treat
                    # print("\n*****填补后，该复诊的治疗计划如下*****\n",later[(later['关联键'].values == i[0])&(later['证件号（id）'].values == i[1])&(later['科室'].values == i[2])&(later['诊断名称'].values == i[3])])
                    else:
                        # print("\n*****出现了问诊之间比初诊还早并且没有具体治疗计划的复诊，我们将其删去，该数据如下*****\n",later[(later['date'].values == x)&(later['关联键'].values == i[0])&(later['证件号（id）'].values == i[1])&(later['科室'].values == i[2])&(later['诊断名称'].values == i[3])])
                        # 没有比复诊更早的初诊，或有可能是被时间窗口截在时间线外
                        later = later[~((later['date'].values == x) & (later['关联键'].values == i[0]) & (
                                later['证件号（id）'].values == i[1]) & (later['科室'].values == i[2]) & (
                                                later['诊断名称'].values == i[3]))]  # ~取反
            else:  # 不存在对应初诊
                # print("\n*****患者{}无对应初诊，应从复诊子集中删去:***** \n{}".format(i,later[(later['关联键'].values == i[0])&(later['证件号（id）'].values == i[1])&(later['科室'].values == i[2])&(later['诊断名称'].values == i[3])]))
                later = later[~((later['关联键'].values == i[0]) & (later['证件号（id）'].values == i[1]) & (
                        later['科室'].values == i[2]) & (later['诊断名称'].values == i[3]))]
                # print("___________________________________________________________________________\n\n")

        print("\n*****重组后该部分初诊数据量为{}，复诊数据为{}*****".format(len(first), len(later)))
        return first, later


    def split_later(self, later):  # 配合数据清理用
        """将未细分过的复诊数据进行进一步切割，切割成不同轮次的复诊。输入的数据必须为未细分过的复诊数据，不得包含初诊。

        """
        names = locals()  # 设定本地变量，使保存的子复诊集可被在循环结束后导出
        data = later.copy()
        frame = pd.DataFrame()  # 空的数据集，用于保存每一个子复诊集

        len_ori = len(data)  # 初始化数据长度，激活第一轮循环
        len_later = len(frame)  # 初始化数据长度，激活第一轮循环
        count = 0  # 计数用以自动对新的数据集变量命名与做if判定
        # print(list(names.keys()))

        while (len_ori - len_later) > 0:  # 若这两个集大小相等时，说明我们已经筛选完了所有复诊子集
            count += 1
            data = data.append(frame).drop_duplicates(keep=False)  # 目的在于第二轮开始我们需要将已经提取出来的子集给删去，对剩下的数据进行进一步的细化，得出下一轮复诊
            # identify2 = list(data[["关联键",'证件号（id）']].values)
            # 用于for循环取出病人的unique病案
            identify2 = data[['关联键', '证件号（id）']].drop_duplicates(keep="first").values  #############20200927
            # l = []
            # for i in (data[["关联键",'证件号（id）']].values):
            #   l.append([i[0],i[1]])
            # for i in l:
            #   if i not in identify2:
            #      identify2.append(i)

            len_ori = len(data)  # 更新被筛选子集的大小，用于下一轮的while条件
            frame = pd.DataFrame()  # 重置空集，因为此变量只为保存每一轮while下，不同层次的子集，所以每次循环需要重置

            # 按照id，先治疗计划，后时间
            for i in identify2:
                pat2 = data[(data["关联键"].values == i[0]) & (data['证件号（id）'].values == i[1])].copy()
                uni_diag_treats = list(pat2[['诊断名称', '治疗计划']].value_counts().index)
                for x in uni_diag_treats:  ###################### 指定了id之后，指定id中不同的治疗计划，因为数据中可能存在多次病程 ###################20200927改
                    pat = pat2[(pat2['治疗计划'].values == x[1]) & (pat2['诊断名称'].values == x[0])]
                    pat = pat.sort_values(by=["date"]).reset_index(drop=True)  # 这里的数据能体现每个病人按时间顺序的病程
                    no1_date1 = pat["date"][0]  # 取第一次问诊的日期数据
                    frame = frame.append(pat[pat["date"].values == no1_date1])

            # 检查复诊子集的“治疗计划”中是否存在“Unknown”。若有，我们以前一次初诊/复诊的治疗计划将其代替
            frame_for_out = frame.copy().astype(object)

            names["data_later{}".format(count)] = frame_for_out  # 设置动态变量，保存筛选出来的这一层复诊
            len_later = len(frame)  # 更新子集长度，用于while条件

        print("here are {} subset for data_later".format(count))  # 提示使用该函数时需要用几个变量来接收返回的子集。

        # 这里用了比较笨拙的方式，猜测我们最终会产生几个复诊子集，我们假设最多不超过4次复诊：
        if count != 0:
            print(list(names.keys())[2:])
            return list(names.values())[2:]

        elif count == 0:
            print("该收费分类数据里没有复诊数据")
            return []  ###若没有进入while循环，说明该消费数据没有任何后续的复诊数据，那么我们返回空集
        else:
            print("something wrong")




    # def load_data(self, path):
    #     '''通过路径，读取zip文件中的单个csv或直接读取csv文件'''
    #     suffix = path.split('.')[-1]
    #     print("The suffix of the file is {}. (this func support 'zip' & 'csv', if not, convert to either)".format(suffix))
    #
    #     if suffix == 'zip':
    #         zipf = ZipFile(path)
    #         nl = zipf.namelist()
    #         with zipf.open(nl[0]) as d:
    #             df0 = pd.read_csv(d, encoding='utf8')
    #
    #         zipf.close()
    #
    #         print("Congrats! The original data was extracted successfully from database! shape: {}".format(df0.shape))
    #         return df0
    #
    #     elif suffix == 'csv':
    #         with open(path) as d:
    #             df0 = pd.read_csv(d, encoding='utf8')
    #
    #         print("Congrats! The original data was extracted successfully from database! shape: {}".format(df0.shape))
    #         return df0
    #
    #     else:
    #         print("Please convert file format to zip or csv")

    def clr_buy(self, data):
        if ("Ca(OH)2" not in data) and ("（" not in data):
            newwords = (data.split("("))[0]
            newwords = newwords.split(".")[0]  # 去除某些项目后面带的句号
        elif ("Ca(OH)2" not in data) and ("（" in data):
            newwords = (data.split("（"))[0]
            newwords = newwords.split(".")[0]

        elif "Ca(OH)2" in data:
            newwords = (data.split("（"))[0]
            newwords = newwords.split(".")[0]

        else:
            newwords = data
            newwords = newwords.split(".")[0]

        return newwords



    def clean1(self):
        data = self.df.copy().reset_index(drop=True)
        data.drop(['Unnamed: 0'],axis = 1,inplace=True)
        data[["诊断名称",'治疗计划','消费项目']] = data[["诊断名称",'治疗计划','消费项目']].astype(str)
        #经过专家确认，单独出现的“同期”为手误输入的“同前”，可将其与同前一起纳为“Unknown”
        #有具体描述的“同期”表述“同时”、“同时期”的含义，所以我们不做更改
        data = data.replace({"nan":"Unknown","同前":"Unknown","同期":"Unknown","同期，":"Unknown"})
        data =  data[data['诊断名称'].values != 'Unknown'] # 诊断名称未知的我们直接去除

        #发现诊断名称中存在以逗号开头的诊断，经问询，可删除字符合并
        data['诊断名称'] = data['诊断名称'].apply(lambda x: x.lstrip(","))
        #string转换时间戳，用以后期时间计算
        data['date'] = pd.to_datetime(data['消费时间']).dt.date

        #将消费项目各项值括号里的个性化描述去除
        data['消费项目'] = data['消费项目'].map(lambda x: self.clr_buy(x))
        return data


    def reform_and_refill(self, dt):
        ###### 重组有初诊的复诊
        first_level1 = dt[dt['初复诊'].values == "初"]
        print("初诊数据量为:{}".format(len(first_level1)))
        # 分好时间戳之后，我们可以将复诊再次细分
        # we have some laters' dt which do not have first HIS records, so we remove them for the accuracy of our analysis

        data_laters = dt[dt["初复诊"].values == "复"]
        print("复诊数据量为:{}".format(len(data_laters)))

        selected_id = []
        havefirst = first_level1[["关联键", "证件号（id）", '诊断名称', '科室']].value_counts().index
        for i in havefirst:
            selected_id += list(data_laters[
                                    (data_laters["关联键"].values == i[0]) & (data_laters['证件号（id）'].values == i[1]) & (
                                                data_laters['诊断名称'].values == i[2]) & (
                                                data_laters['科室'].values == i[3])].index)  # 有初诊的复诊

        df = data_laters.loc[selected_id, :]

        print("拥有初诊的复诊总数据量为:{}".format(len(df)))

        ###
        real_later = df[df['治疗计划'].values == "Unknown"]  # 有初诊的复诊中真正的复诊  ### 如牙列不齐，很可能初复诊治疗计划都是unknown
        print("有初诊的复诊中真正的复诊:{}".format(len(real_later)))
        ###
        first_level2 = df[df['治疗计划'].values != "Unknown"]  # 有初诊的复诊中的伪复诊，将被合并到初诊中
        print("有初诊的复诊中的伪复诊:{}".format(len(first_level2)))

        find_real_candi = self.later_to_first(first_level1, first_level2)
        real_first = first_level1.append(find_real_candi[0]).reset_index(drop=True)
        real_later = real_later.append(find_real_candi[1]).reset_index(drop=True)
        print("至此，部分真初诊%d条，部分真复诊%d条，一共%d条" % (len(real_first), len(real_later), len(real_first) + len(real_later)))

        ###### 填补有初诊的复诊中unknown治疗计划
        fillup1 = self.refill_treat(real_first, real_later)
        real_first = fillup1[0]
        real_later = fillup1[1]

        ###### 重组无初诊的复诊
        # 第一部分的初复诊已被分割、重组并填补治疗计划，接下来我们要将“无初诊的复诊数据”中的伪复诊重组为初诊，并且为他们的复诊填补治疗计划
        nofirst_laters = data_laters.append(df).drop_duplicates(keep=False)  # 筛选出没有初诊的复诊
        nofirst_laters.loc[nofirst_laters['治疗计划'] != "Unknown", "初复诊"] = "初"  # 从没有初诊的复诊集中，找出自身就有具体治疗计划的伪复诊，并且换成初诊标签
        # 最后，根据初复诊标签，将没有初诊的复诊总集分割成第二部分的初复诊子集
        nofirst_first = nofirst_laters[nofirst_laters['初复诊'] == '初']
        nofirst_laters = nofirst_laters[nofirst_laters['初复诊'] == '复']

        find_real_candi0 = self.later_to_first(nofirst_first, nofirst_laters)
        nofirst_first = nofirst_first.append(find_real_candi0[0]).reset_index(drop=True)
        nofirst_laters = find_real_candi0[1].reset_index(drop=True)
        print("至此，部分真初诊%d条，部分真复诊%d条，一共%d条" % (
        len(nofirst_first), len(nofirst_laters), len(nofirst_first) + len(nofirst_laters)))

        ###### 填补无初诊的复诊中unknown治疗计划
        fillup2 = self.refill_treat(nofirst_first, nofirst_laters)
        nofirst_first = fillup2[0]
        nofirst_laters = fillup2[1]

        ###### 合并重组、填补完毕的针初复诊子集
        # 以下将两部分初复诊数据合并，成为最后的初复诊集
        data_first = real_first.append(nofirst_first).reset_index(drop=True)
        data_laters = real_later.append(nofirst_laters).reset_index(drop=True)

        print("初复诊数据重组完毕，一共有新的初诊数据{}条，新的复诊数据有{}条".format(len(data_first), len(data_laters)))

        return data_first, data_laters


    def group(self, data):
        return data.groupby(['关联键', '证件号（id）', '诊断名称', '治疗计划', '科室', 'date'])['消费项目'].unique().reset_index(
            drop=False)


    def link_first_laters(self, first, laters):

        all_tuple = ()
        restructured = [first] + [i for i in laters]  # 初诊+所有复诊

        for dataset in restructured:
            all_tuple += (self.group(dataset),)

        final_out = all_tuple[0]
        for i in range(1, len(all_tuple)):
            final_out = final_out.join(all_tuple[i].set_index(["关联键", "证件号（id）", '诊断名称', '治疗计划', '科室']), how='left',
                                       on=["关联键", "证件号（id）", '诊断名称', '治疗计划', '科室'], rsuffix="_复{}".format(i))

        return final_out


    def start_link(self):
        fl = self.reform_and_refill(self.clean1())
        output = self.link_first_laters(fl[0], self.split_later(fl[1]))

        return output