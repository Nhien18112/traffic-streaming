import torch
import torch.nn as nn
from torch_geometric.nn import GCNConv


# class TGCN(nn.Module):
#     def __init__(self, num_node_features: int, hidden_dim: int = 32, num_horizons: int = 1):
#         super(TGCN, self).__init__()
#         self.gcn = GCNConv(num_node_features, hidden_dim)
#         self.gru = nn.GRU(input_size=hidden_dim, hidden_size=hidden_dim, batch_first=True)  # TINH CHỈNH ĐỂ TỐT HƠN
#         self.linear = nn.Linear(hidden_dim, num_horizons)

#     def forward(self, x: torch.Tensor, edge_index: torch.Tensor, edge_weight: torch.Tensor) -> torch.Tensor:
#         batch_size, window_size, num_nodes, num_features = x.shape
#         gcn_outputs = []

#         for t in range(window_size):
#             xt = x[:, t, :, :].reshape(-1, num_features)
#             out_gcn = self.gcn(xt, edge_index, edge_weight)
#             out_gcn = out_gcn.view(batch_size, num_nodes, -1)
#             gcn_outputs.append(out_gcn)

#         gcn_outputs = torch.stack(gcn_outputs, dim=1)
#         gru_input = gcn_outputs.permute(0, 2, 1, 3).reshape(batch_size * num_nodes, window_size, -1)
#         gru_out, _ = self.gru(gru_input)
#         last_hidden = gru_out[:, -1, :]
#         pred = self.linear(last_hidden)
#         return pred.view(batch_size, num_nodes, -1)


class TGCN(nn.Module):
    def __init__(self, num_node_features: int, hidden_dim: int = 32, num_horizons: int = 1):
        super(TGCN, self).__init__()
        self.gcn = GCNConv(num_node_features, hidden_dim)
        
        # Tinh chỉnh 1: Tăng lên 2 lớp GRU để nắm bắt chuỗi thời gian dài tốt hơn
        self.gru = nn.GRU(
            input_size=hidden_dim, 
            hidden_size=hidden_dim, 
            num_layers=2,           # Thay vì 1 lớp như cũ
            batch_first=True
        ) 
        
        # Tinh chỉnh 2: Thêm một lớp đệm phi tuyến tính trước khi output
        self.decoder = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_horizons)
        )

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, edge_weight: torch.Tensor) -> torch.Tensor:
        batch_size, window_size, num_nodes, num_features = x.shape
        gcn_outputs = []

        for t in range(window_size):
            xt = x[:, t, :, :].reshape(-1, num_features)
            out_gcn = self.gcn(xt, edge_index, edge_weight)
            out_gcn = out_gcn.view(batch_size, num_nodes, -1)
            gcn_outputs.append(out_gcn)

        gcn_outputs = torch.stack(gcn_outputs, dim=1)
        gru_input = gcn_outputs.permute(0, 2, 1, 3).reshape(batch_size * num_nodes, window_size, -1)
        
        gru_out, _ = self.gru(gru_input)
        last_hidden = gru_out[:, -1, :]
        
        # Sử dụng decoder thay cho linear đơn thuần
        pred = self.decoder(last_hidden)
        
        return pred.view(batch_size, num_nodes, -1)
