import os
import re
import ee
from datetime import datetime, timedelta
import geemap

project_id = 'empyrean-caster-430308-m2'
ee.Initialize(project=project_id)

def VNP46A2_angular_correction_tool(
        study_area: str,
        scale_level: str,
        time_range_input: str,
):
    """
            Perform angular effect correction on NASA VNP46A2 daily NTL data from Google Earth Engine,
            using 16-group mean normalization to remove sensor zenith angle effects, and output the corrected
            image collection with pixel-wise mean values over the specified date range.

            Parameters:
            - study_area (str): Name of the target region. For China, use Chinese names (e.g., 江苏省, 南京市).
            - scale_level (str): Administrative level ('country', 'province', 'city', 'county').
            - time_range_input (str): Date range in 'YYYY-MM-DD to YYYY-MM-DD' format.
    """
    # Set administrative boundary dataset based on scale level
    national_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/World_countries")
    province_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/province")
    city_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/city")
    county_collection = ee.FeatureCollection("projects/empyrean-caster-430308-m2/assets/county")

    # Select administrative boundaries
    def get_administrative_boundaries(scale_level):
        # Handle directly governed cities as province-level data in China
        directly_governed_cities = ['北京市', '天津市', '上海市', '重庆市']
        if scale_level == 'province' or (scale_level == 'city' and study_area in directly_governed_cities):
            admin_boundary = province_collection
            name_property = 'name'
        elif scale_level == 'country':
            admin_boundary = national_collection
            name_property = 'NAME'
        elif scale_level == 'city':
            admin_boundary = city_collection
            name_property = 'name'
        elif scale_level == 'county':
            admin_boundary = county_collection
            name_property = 'name'
        else:
            raise ValueError("Unknown scale level. Options are 'country', 'province', 'city', or 'county'.")
        return admin_boundary, name_property

    admin_boundary, name_property = get_administrative_boundaries(scale_level)
    region = admin_boundary.filter(ee.Filter.eq(name_property, study_area))
    # Validate region
    if region.size().getInfo() == 0:
        raise ValueError(f"No area named '{study_area}' found under scale level '{scale_level}'.")
    region = region.geometry()


    # if region.isEmpty().getInfo():
    #     raise ValueError(f"No area named '{study_area}' found under scale level '{scale_level}'.")

    # Parse time range
    def parse_time_range(time_range_input):
        time_range_input = time_range_input.replace(' ', '')
        if 'to' in time_range_input:
            start_str, end_str = time_range_input.split('to')
            start_str, end_str = start_str.strip(), end_str.strip()
        else:
            # Single date input
            start_str = end_str = time_range_input.strip()

        if not re.match(r'^\d{4}-\d{2}-\d{2}$', start_str) or not re.match(r'^\d{4}-\d{2}-\d{2}$', end_str):
            raise ValueError("Invalid daily format. Use 'YYYY-MM-DD' or 'YYYY-MM-DD to YYYY-MM-DD'.")
        start_date, end_date = start_str, end_str

        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            raise ValueError("Start date cannot be later than end date.")

        return start_date, end_date

    start_date, end_date = parse_time_range(time_range_input)

    NTL_collection = (
        ee.ImageCollection('NASA/VIIRS/002/VNP46A2')
        .filterDate(start_date, (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
        .select('DNB_BRDF_Corrected_NTL')
        .filterBounds(region)
        .map(lambda image: image.clip(region))
    )

    # ========== 计算每个影像的组号 ==========

    def add_group_number(image):
        # 计算影像日期
        date = ee.Date(image.get('system:time_start'))
        # 计算组号（0-15）
        days_diff = date.difference(ee.Date(start_date), 'day')
        group_number = days_diff.mod(16).int()
        # 将组号添加到影像属性中
        return image.set('group_number', group_number)

    # 将函数应用到影像集合中
    viirs_collection = NTL_collection.map(add_group_number)
    # ========== 数据预处理：消除传感器角度影响 ==========

    # ========== 实现逐像素角度效应校正 ==========

    # **步骤1：计算年度逐像元均值影像 N**

    # 注意处理空值，使用 ee.Reducer.mean() 会自动忽略空值
    annual_mean_image = viirs_collection.mean()

    # **步骤2：按组号分组影像集合，计算每个组的逐像元均值影像 N1, N2, ..., N16**

    group_numbers = ee.List.sequence(0, 15)

    def compute_group_mean_image(group_number):
        group_number = ee.Number(group_number)
        group_collection = viirs_collection.filter(ee.Filter.eq('group_number', group_number))
        group_mean_image = group_collection.mean()
        # 将组号添加到影像属性中
        return group_mean_image.set('group_number', group_number)

    # 计算每个组的平均影像，并生成一个 ImageCollection
    group_mean_images = ee.ImageCollection(group_numbers.map(compute_group_mean_image))

    # **步骤3：计算每个组的角度效应系数影像 Ai = Ni / N**

    def compute_correction_image(image):
        group_number = image.get('group_number')
        group_mean_image = image
        # 计算校正系数影像 Ai = Ni / N
        correction_image = group_mean_image.divide(annual_mean_image).unmask(1)
        # 将组号添加到校正影像属性中
        return correction_image.set('group_number', group_number)

    # 生成校正系数影像集合
    correction_images = group_mean_images.map(compute_correction_image)

    # **步骤4：对每个组的影像集合进行校正**

    def correct_group_images(group_number):
        group_number = ee.Number(group_number)
        # 获取对应组号的校正系数影像 Ai
        correction_image = correction_images.filter(ee.Filter.eq('group_number', group_number)).first()
        # 获取对应组号的影像集合
        group_collection = viirs_collection.filter(ee.Filter.eq('group_number', group_number))
        # 对组内的每个影像进行校正
        corrected_group = group_collection.map(lambda image: image.divide(correction_image)
                                               .copyProperties(image, image.propertyNames()))
        return corrected_group

    # 对每个组进行校正，得到校正后的影像集合列表
    corrected_groups = group_numbers.map(correct_group_images)

    # 将每个 ImageCollection 展开成单个 Image 列表并合并为一个 ImageCollection
    all_corrected_images = ee.ImageCollection(corrected_groups.iterate(
        lambda img_col, acc: ee.ImageCollection(acc).merge(ee.ImageCollection(img_col)),
        ee.ImageCollection([])
    ))

    # 生成最终的 corrected_collection
    corrected_collection = all_corrected_images

    # **步骤5：按日期排序**

    corrected_collection = corrected_collection.sort('system:time_start')
    def set_filename(image):
        date = ee.Date(image.get('system:time_start')).format('yyyy_MM_dd')
        file_name = ee.String(study_area).cat('_').cat(date)
        return image.set('file_name', file_name)

    corrected_collection = corrected_collection.map(set_filename)
    # 从集合中提取所有 file_name
    filenames = corrected_collection.aggregate_array('file_name').getInfo()
    print(filenames)
    # # --------- 导出 corrected_collection 到本地 ---------
    # out_dir = r"C:\NTL_Agent\Night_data\GEE"
    # os.makedirs(out_dir, exist_ok=True)
    #
    # # 显式设置导出参数
    # export_scale = 500  # VNP46A2 分辨率
    # export_crs = 'EPSG:4326'  # 如需保持原始投影，可改为 image.projection()
    # export_region = region  # 约束导出范围
    # file_per_band = False
    #
    # # 逐影像导出（更稳），使用我们已设置的 file_name 属性
    # image_list = corrected_collection.toList(corrected_collection.size())
    # n = corrected_collection.size().getInfo()
    #
    # for i in range(n):
    #     img = ee.Image(image_list.get(i))
    #     fname = img.get('file_name').getInfo()  # e.g., 上海市_2020_01_05
    #     out_path = os.path.join(out_dir, f"{fname}.tif")
    #
    #     geemap.ee_export_image(
    #         ee_object=img,
    #         filename=out_path,
    #         scale=export_scale,
    #         region=export_region,
    #         crs=export_crs,
    #         file_per_band=file_per_band,
    #         # maxPixels/timeout 依 geemap 版本可选：
    #         # max_pixels=1e13
    #     )
    # print(f"The preprocessed VNP46A2 data has been saved in {out_dir}")
    # return f"The preprocessed VNP46A2 data has been saved in {out_dir}."

    # --------- 导出 corrected_collection 到asset ---------
    ASSET_FOLDER = "projects/empyrean-caster-430308-m2/assets/VNP46A2_corr_SH_20200101_20200301"

    # 若不存在则创建（幂等）
    try:
        geemap.ee_create_folder(ASSET_FOLDER)
    except Exception:
        pass

    # 3) 导出参数
    export_scale = 500  # VNP46A2 分辨率 500m
    export_crs = 'EPSG:4326'  # 或使用原生投影: ee.Image(image_list.get(0)).projection().crs()
    export_region = region  # 你上文已得到 region = region.geometry()

    # 4) 资产命名需要 ASCII，尽量别用中文 & 特殊字符
    def to_safe_name(s: str) -> str:
        # 只保留字母数字和下划线、短横线
        s = re.sub(r'[^A-Za-z0-9_\-]', '_', s)
        # Asset 名称一般不要太长，截断到 120 字符以内更稳妥
        return s[:120]

    image_list = corrected_collection.toList(corrected_collection.size())
    n = corrected_collection.size().getInfo()

    tasks = []
    for i in range(n):
        img = ee.Image(image_list.get(i))
        # 读取我们之前 set 的 file_name: 例如 "上海市_2020_01_05"
        raw_name = img.get('file_name').getInfo()
        # 生成安全的资产名，例如 "SH_2020_01_05" 或 "Shanghai_2020_01_05"
        # 这里演示把中文替换为 ascii 安全名
        # 如果你想保留“上海市”，可以在生成 file_name 时就用英文 "Shanghai"
        safe_name = to_safe_name(raw_name)

        asset_id = f"{ASSET_FOLDER}/{safe_name}"  # 完整 Asset 路径
        desc = f"VNP46A2_corr_{safe_name}"

        task = ee.batch.Export.image.toAsset(
            image=img,
            description=desc,
            assetId=asset_id,
            region=export_region,
            scale=export_scale,
            crs=export_crs,
            maxPixels=1e13
        )
        task.start()
        tasks.append(task)
        time.sleep(1.0)  # 轻微节流，避免瞬时提交过多任务
    print(f"Submitted {len(tasks)} export tasks to Asset folder:\n  {ASSET_FOLDER}")

# 确保已 ee.Initialize() 且已安装 geemap
# VNP46A2_angular_correction_tool(
#     study_area="上海市",
#     scale_level="city",
#     time_range_input="2020-01-01 to 2020-03-01",
# )
