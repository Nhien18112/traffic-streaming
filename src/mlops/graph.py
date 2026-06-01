import torch
from torch_geometric.nn.conv.gcn_conv import gcn_norm

INTERSECTIONS = {
    "Nut_Giao_Truong_Chinh_Cong_Hoa": (10.807512, 106.634811),
    "Nut_Giao_Truong_Chinh_Au_Co": (10.80184, 106.63674),
    "Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham": (10.80184, 106.64742),
    "Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham": (10.79637, 106.64694),
    "Duong_Au_Co": (10.78938, 106.6404),
    "Lang_Cha_Ca": (10.800619, 106.661099),
    "Nga_Tu_Bay_Hien": (10.792951, 106.653439),
    "Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet": (10.79034, 106.65236),
    "Nut_Giao_Lac_Long_Quan_Au_Co": (10.77478, 106.64798),
    "Nut_Giao_Bac_Hai_CMT8": (10.786912, 106.664607),
    "Nut_Giao_Bac_Hai_Thanh_Thai": (10.78082, 106.65883),
    "Nut_Giao_LTK_Bac_Hai": (10.778018, 106.656201),
    "Nut_Giao_Lu_Gia_Nguyen_Thi_Nho": (10.77115, 106.65292),
    "Nut_Giao_Au_Co_Nguyen_Thi_Nho": (10.76906, 106.65226),
    "Nut_Giao_To_Hien_Thanh_CMT8": (10.782902, 106.672072),
    "Nut_Giao_Su_Van_Hanh_To_Hien_Thanh": (10.77795, 106.66557),
    "Nga_Tu_Thanh_Thai_To_Hien_Thanh": (10.77639, 106.66353),
    "Nut_Giao_LTK_To_Hien_Thanh": (10.77045, 106.658259),
    "Bung_binh_dan_chu": (10.77781, 106.681391),
    "Nut_Giao_3Thang2_Cao_Thang": (10.77381, 106.67778),
    "Nut_Giao_3Thang2_Le_Hong_Phong": (10.77091, 106.67311),
    "Nut_Giao_3Thang2_Su_Van_Hanh": (10.76962, 106.67076),
    "Nut_Giao_3Thang2_Ly_Thai_To": (10.7681, 106.66803),
    "Nut_Giao_3Thang2_Thanh_Thai": (10.76778, 106.66702),
    "Nut_Giao_Ly_Thuong_Kiet_3Thang2": (10.763875, 106.660007),
    "Nut_Giao_3Thang2_Le_Dai_Hanh": (10.76226, 106.65707),
    "Nut_Giao_CMT8_Dien_Bien_Phu": (10.77676, 106.68371),
    "Nut_Giao_Cao_Thang_Dien_Bien_Phu": (10.77276, 106.679),
    "Bung_binh_Ly_Thai_To": (10.76776, 106.67418)
}

node_list = list(INTERSECTIONS.keys())
node_to_idx = {name: idx for idx, name in enumerate(node_list)}

EDGES_LIST = [
    ('Nut_Giao_Truong_Chinh_Cong_Hoa', 'Nut_Giao_Truong_Chinh_Au_Co', True),
    ('Nut_Giao_Truong_Chinh_Cong_Hoa', 'Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham', True),
    ('Nut_Giao_Truong_Chinh_Au_Co', 'Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham', True),
    ('Nut_Giao_Truong_Chinh_Au_Co', 'Duong_Au_Co', True),
    ('Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham', 'Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham', True),
    ('Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham', 'Lang_Cha_Ca', True),
    ('Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham', 'Nga_Tu_Bay_Hien', True),
    ('Duong_Au_Co', 'Nut_Giao_Lac_Long_Quan_Au_Co', True),
    ('Lang_Cha_Ca', 'Nga_Tu_Bay_Hien', True),
    ('Nga_Tu_Bay_Hien', 'Nut_Giao_Bac_Hai_CMT8', True),
    ('Nga_Tu_Bay_Hien', 'Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet', True),
    ('Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet', 'Nut_Giao_Lac_Long_Quan_Au_Co', True),
    ('Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet', 'Nut_Giao_LTK_Bac_Hai', True),
    ('Nut_Giao_Lac_Long_Quan_Au_Co', 'Nut_Giao_Au_Co_Nguyen_Thi_Nho', True),
    ('Nut_Giao_Bac_Hai_CMT8', 'Nut_Giao_Bac_Hai_Thanh_Thai', True),
    ('Nut_Giao_Bac_Hai_CMT8', 'Nut_Giao_To_Hien_Thanh_CMT8', True),
    ('Nut_Giao_Bac_Hai_Thanh_Thai', 'Nut_Giao_LTK_Bac_Hai', True),
    ('Nut_Giao_Bac_Hai_Thanh_Thai', 'Nga_Tu_Thanh_Thai_To_Hien_Thanh', True),
    ('Nut_Giao_LTK_Bac_Hai', 'Nut_Giao_LTK_To_Hien_Thanh', True),
    ('Nut_Giao_LTK_To_Hien_Thanh', 'Nut_Giao_Lu_Gia_Nguyen_Thi_Nho', True),
    ('Nut_Giao_LTK_To_Hien_Thanh', 'Nut_Giao_Ly_Thuong_Kiet_3Thang2', True),
    ('Nut_Giao_Lu_Gia_Nguyen_Thi_Nho', 'Nut_Giao_Au_Co_Nguyen_Thi_Nho', True),
    ('Nut_Giao_Au_Co_Nguyen_Thi_Nho', 'Nut_Giao_3Thang2_Le_Dai_Hanh', True),
    ('Nut_Giao_To_Hien_Thanh_CMT8', 'Nut_Giao_Su_Van_Hanh_To_Hien_Thanh', True),
    ('Nut_Giao_To_Hien_Thanh_CMT8', 'Bung_binh_dan_chu', True),
    ('Nut_Giao_Su_Van_Hanh_To_Hien_Thanh', 'Nga_Tu_Thanh_Thai_To_Hien_Thanh', True),
    ('Nut_Giao_Su_Van_Hanh_To_Hien_Thanh', 'Nut_Giao_3Thang2_Su_Van_Hanh', True),
    ('Nga_Tu_Thanh_Thai_To_Hien_Thanh', 'Nut_Giao_LTK_To_Hien_Thanh', True),
    ('Nga_Tu_Thanh_Thai_To_Hien_Thanh', 'Nut_Giao_Ly_Thuong_Kiet_3Thang2', True),
    ('Bung_binh_dan_chu', 'Nut_Giao_3Thang2_Cao_Thang', True),
    ('Bung_binh_dan_chu', 'Nut_Giao_CMT8_Dien_Bien_Phu', True),
    ('Nut_Giao_3Thang2_Cao_Thang', 'Nut_Giao_3Thang2_Le_Hong_Phong', True),
    ('Nut_Giao_3Thang2_Cao_Thang', 'Nut_Giao_Cao_Thang_Dien_Bien_Phu', True),
    ('Nut_Giao_3Thang2_Le_Hong_Phong', 'Nut_Giao_3Thang2_Su_Van_Hanh', True),
    ('Nut_Giao_3Thang2_Le_Hong_Phong', 'Bung_binh_Ly_Thai_To', True),
    ('Nut_Giao_3Thang2_Su_Van_Hanh', 'Nut_Giao_3Thang2_Ly_Thai_To', True),
    ('Nut_Giao_3Thang2_Ly_Thai_To', 'Nut_Giao_3Thang2_Thanh_Thai', True),
    ('Nut_Giao_3Thang2_Ly_Thai_To', 'Bung_binh_Ly_Thai_To', True),
    ('Nut_Giao_3Thang2_Thanh_Thai', 'Nut_Giao_Ly_Thuong_Kiet_3Thang2', True),
    ('Nut_Giao_Ly_Thuong_Kiet_3Thang2', 'Nut_Giao_3Thang2_Le_Dai_Hanh', True),
    ('Bung_binh_Ly_Thai_To', 'Nut_Giao_Cao_Thang_Dien_Bien_Phu', False),
    ('Nut_Giao_Cao_Thang_Dien_Bien_Phu', 'Nut_Giao_CMT8_Dien_Bien_Phu', False)
]


def calculate_haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1, phi2 = torch.deg2rad(lat1), torch.deg2rad(lat2)
    dphi = torch.deg2rad(lat2 - lat1)
    dlam = torch.deg2rad(lon2 - lon1)
    a = torch.sin(dphi / 2) ** 2 + torch.cos(phi1) * torch.cos(phi2) * torch.sin(dlam / 2) ** 2
    return R * 2 * torch.atan2(torch.sqrt(a), torch.sqrt(1 - a))


def build_graph():
    source_nodes = []
    target_nodes = []

    for u, v, is_two_way in EDGES_LIST:
        idx_u = node_to_idx[u]
        idx_v = node_to_idx[v]
        source_nodes.append(idx_u)
        target_nodes.append(idx_v)

        if is_two_way:
            source_nodes.append(idx_v)
            target_nodes.append(idx_u)

    edge_index = torch.tensor([source_nodes, target_nodes], dtype=torch.long)
    coords = torch.tensor(list(INTERSECTIONS.values()), dtype=torch.float32)
    u_coords = coords[edge_index[0]]
    v_coords = coords[edge_index[1]]
    distances = calculate_haversine(u_coords[:, 0], u_coords[:, 1], v_coords[:, 0], v_coords[:, 1])
    edge_weight = 1.0 / (distances + 1e-5)
    w_min, w_max = edge_weight.min(), edge_weight.max()
    edge_weight = (edge_weight - w_min) / (w_max - w_min + 1e-5)

    edge_index, edge_weight = gcn_norm(edge_index, edge_weight, num_nodes=len(node_list))
    return edge_index, edge_weight


edge_index, edge_weight = build_graph()
