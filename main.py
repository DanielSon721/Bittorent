import argparse
from torrent import TorrentMeta
from tracker import TrackerClient
from client import BitTorrentClient

def parse_args():
    parser = argparse.ArgumentParser(description="Simple BitTorrent client")
    parser.add_argument("torrent", help=".torrent file path")
    parser.add_argument("-o", "--output", help="Output file path (optional)")
    parser.add_argument("--port", type=int, default=6881, help="Listening port")
    return parser.parse_args()

def main():
    args = parse_args()

    torrent = TorrentMeta.from_file(args.torrent)
    peer_id = torrent.generate_peer_id()

    client = BitTorrentClient(
        torrent=torrent,
        peer_id=peer_id,
        listen_port=args.port,
        output_path=args.output,
    )

    client.run()

if __name__ == "__main__":
    main()
