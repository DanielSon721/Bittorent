import argparse
import logging
from torrent import TorrentMeta
from tracker import TrackerClient
from client import BitTorrentClient

def parse_args():
    parser = argparse.ArgumentParser(description="Simple BitTorrent client")
    parser.add_argument("torrent", help=".torrent file path")
    parser.add_argument("-o", "--output", help="Output file path (optional)")
    parser.add_argument("--port", type=int, default=6881, help="Listening port")
    parser.add_argument(
        "--peer",
        action="append",
        help="Manual peer in ip:port form (can be given multiple times)",
    )
    return parser.parse_args()

def prompt_mode() -> str:
    while True:
        choice = input("Choose mode: (d)ownload or (u)pload? [d/u]: ").strip().lower()
        if choice in ("d", "download"):
            return "download"
        if choice in ("u", "upload"):
            return "upload"
        print("Please enter 'd' for download or 'u' for upload.")

def prompt_verbose() -> bool:
    while True:
        choice = input("Enable verbose logging? [y/n]: ").strip().lower()
        if choice in ("y", "yes"):
            return True
        if choice in ("n", "no"):
            return False
        print("Please enter 'y' or 'n'.")

def main():
    args = parse_args()

    mode = prompt_mode()
    verbose = prompt_verbose()
    print(f"Verbose mode: {'on' if verbose else 'off'}")

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    torrent = TorrentMeta.from_file(args.torrent)
    peer_id = torrent.generate_peer_id()

    if mode == "upload":
        payload_path = args.output or torrent.name
        client = BitTorrentClient(
            torrent=torrent,
            peer_id=peer_id,
            listen_port=args.port,
            output_path=payload_path,
            seed_mode=True,
            extra_peers=args.peer,
        )
        client.run()
        return

    client = BitTorrentClient(
        torrent=torrent,
        peer_id=peer_id,
        listen_port=args.port,
        output_path=args.output,
        extra_peers=args.peer,
    )

    client.run()

if __name__ == "__main__":
    main()
