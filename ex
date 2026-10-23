#include "protocol.h"

static int send_data_frame(int sock, int seq) {
    return send_packet(sock, TYPE_DATA, seq);
}

static void reset_gbn_window(int base, int end, int sent[]) {
    for (int i = base; i < end; i++) {
        sent[i] = 0;
    }
}

static int create_server_socket(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        perror("socket");
        return -1;
    }

    int opt = 1;

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, 1) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    printf("Server listening on port %d...\n", PORT);

    int client_fd = accept(fd, NULL, NULL);

    if (client_fd < 0) {
        perror("accept");
        close(fd);
        return -1;
    }

    printf("Client connected!\n\n");
    close(fd);

    return client_fd;
}

static int send_config(
    int sock,
    int protocol,
    int sim_type,
    int error_frame,
    int total_frames,
    int window_size
) {
    struct Config cfg;

    cfg.protocol = protocol;
    cfg.sim_type = sim_type;
    cfg.error_frame = error_frame;
    cfg.total_frames = total_frames;
    cfg.window_size = window_size;

    return send_all(sock, &cfg, sizeof(cfg));
}

static void run_gbn(
    int sock,
    int total_frames,
    int window_size,
    int sim_type,
    int error_frame
) {
    int base = 0;
    int next_seq = 0;

    int sent[MAX_FRAMES] = {0};
    int acked[MAX_FRAMES] = {0};

    int data_loss_done = 0;
    int64_t timer_start = 0;

    printf("--- STARTING GO-BACK-N SIMULATION ---\n");

    while (base < total_frames) {
        int end = base + window_size;

        if (end > total_frames) {
            end = total_frames;
        }

        print_window(base, window_size, total_frames, acked);

        /*
         * Transmit no more than three new frames per batch.
         * The overall outstanding window can be larger than three.
         */
        int batch_count = 0;

        while (next_seq < end && batch_count < MAX_BATCH) {
            int seq = next_seq++;

            if (sim_type == SCENARIO_DATA_LOSS &&
                seq == error_frame && !data_loss_done) {

                printf("[Server] Sending Frame %d"
                       " -> [DATA LOST IN NETWORK]\n",
                       display_seq(seq, window_size));

                data_loss_done = 1;
            } else {
                printf("[Server] Sending Frame %d\n", display_seq(seq, window_size));

                if (send_data_frame(sock, seq) < 0) {
                    perror("send DATA");
                    return;
                }
            }

            sent[seq] = 1;
            batch_count++;

            if (base == seq) {
                timer_start = monotonic_ms();
            }
        }

        /*
         * No outstanding frame means we can immediately continue
         * transmitting within the available window.
         */
        if (base >= next_seq) {
            continue;
        }

        int64_t elapsed = monotonic_ms() - timer_start;
        int remaining = GBN_TIMEOUT_MS - (int)elapsed;

        if (remaining < 0) {
            remaining = 0;
        }

        int ready = wait_for_data_ms(sock, remaining);

        if (ready < 0) {
            perror("select");
            return;
        }

        if (ready == 0) {
            printf("\n[Server] *** TIMEOUT (3s)! "
                   "Outstanding base frame: %d ***\n",
                   display_seq(base, window_size));

            printf("[Server] GBN: Retransmitting outstanding "
                   "frames from Base %d\n", display_seq(base, window_size));

            /*
             * Retransmit only frames currently outstanding.
             * Reset next_seq so the active window is retransmitted
             * in batches of at most three.
             */
            reset_gbn_window(base, next_seq, sent);
            next_seq = base;
            timer_start = monotonic_ms();

            continue;
        }

        struct Packet reply;

        if (recv_packet(sock, &reply) < 0) {
            printf("[Server] Client disconnected or invalid packet.\n");
            return;
        }

        if (reply.type == TYPE_ACK) {
            printf("[Server] Received Cumulative ACK %d\n",
                   display_seq(reply.seq, window_size));

            /*
             * ACK n acknowledges all frames through n.
             * Never accept ACKs for unsent frames.
             */
            if (reply.seq >= base &&
                reply.seq < next_seq &&
                reply.seq < total_frames) {

                for (int i = base; i <= reply.seq; i++) {
                    acked[i] = 1;
                }

                base = reply.seq + 1;
                timer_start = monotonic_ms();

                print_window(base, window_size,
                             total_frames, acked);
            } else if (reply.seq < base) {
                printf("[Server] Ignoring duplicate/stale ACK %d\n",
                       display_seq(reply.seq, window_size));
            } else {
                printf("[Server] Ignoring invalid/future ACK %d\n",
                       display_seq(reply.seq, window_size));
            }
        } else if (reply.type == TYPE_NAK) {
            printf("[Server] Received Cumulative NAK %d\n",
                   display_seq(reply.seq, window_size));

            if (reply.seq >= base &&
                reply.seq < next_seq &&
                reply.seq < total_frames) {

                printf("[Server] GBN recovery: retransmitting "
                       "from Base %d\n", display_seq(base, window_size));

                reset_gbn_window(base, next_seq, sent);
                next_seq = base;
                timer_start = monotonic_ms();
            } else {
                printf("[Server] Ignoring invalid NAK.\n");
            }
        }
    }

    printf("\n[Server] All %d frames acknowledged successfully!\n",
           total_frames);
}

static void run_sr(
    int sock,
    int total_frames,
    int window_size,
    int sim_type,
    int error_frame
) {
    int base = 0;
    int next_seq = 0;

    int sent[MAX_FRAMES] = {0};
    int acked[MAX_FRAMES] = {0};

    int data_loss_done = 0;
    int64_t sent_time[MAX_FRAMES] = {0};

    printf("--- STARTING SELECTIVE REPEAT SIMULATION ---\n");

    while (base < total_frames) {
        int end = base + window_size;

        if (end > total_frames) {
            end = total_frames;
        }

        print_window(base, window_size, total_frames, acked);

        /*
         * Fill the sender's window with new frames.
         * A simulated lost frame is marked as sent so that its
         * timer eventually triggers retransmission.
         */
        while (next_seq < end) {
            int seq = next_seq++;

            if (sim_type == SCENARIO_DATA_LOSS &&
                seq == error_frame && !data_loss_done) {

                printf("[Server] Sending Frame %d"
                       " -> [DATA LOST IN NETWORK]\n",
                       display_seq(seq, window_size));

                data_loss_done = 1;
            } else {
                printf("[Server] Sending Frame %d\n", display_seq(seq, window_size));

                if (send_data_frame(sock, seq) < 0) {
                    perror("send DATA");
                    return;
                }
            }

            sent[seq] = 1;
            sent_time[seq] = monotonic_ms();
        }

        /*
         * Find the earliest timeout among outstanding frames.
         * Each frame has its own timer.
         */
        int timeout_ms = SR_TIMEOUT_MS;
        int64_t now = monotonic_ms();

        for (int i = base; i < next_seq; i++) {
            if (sent[i] && !acked[i]) {
                int64_t elapsed = now - sent_time[i];
                int remaining = SR_TIMEOUT_MS - (int)elapsed;

                if (remaining < timeout_ms) {
                    timeout_ms = remaining;
                }
            }
        }

        if (timeout_ms < 0) {
            timeout_ms = 0;
        }

        int ready = wait_for_data_ms(sock, timeout_ms);

        if (ready < 0) {
            perror("select");
            return;
        }

        if (ready == 0) {
            now = monotonic_ms();

            /*
             * Retransmit only frames whose individual timers expired.
             * ACKed frames are never retransmitted.
             */
            for (int i = base; i < next_seq; i++) {
                if (sent[i] && !acked[i] &&
                    now - sent_time[i] >= SR_TIMEOUT_MS) {

                    printf("\n[Server] *** TIMEOUT! "
                           "Retransmitting un-ACKed Frame %d ***\n",
                           display_seq(i, window_size));

                    if (send_data_frame(sock, i) < 0) {
                        perror("send retransmission");
                        return;
                    }

                    sent_time[i] = monotonic_ms();
                }
            }

            continue;
        }

        struct Packet reply;

        if (recv_packet(sock, &reply) < 0) {
            printf("[Server] Client disconnected or invalid packet.\n");
            return;
        }

        if (reply.seq < 0 || reply.seq >= total_frames) {
            printf("[Server] Ignoring invalid sequence number %d\n",
                   display_seq(reply.seq, window_size));
            continue;
        }

        if (reply.type == TYPE_ACK) {
            printf("[Server] Received ACK %d\n",
                   display_seq(reply.seq, window_size));

            if (reply.seq >= next_seq || !sent[reply.seq]) {
                printf("[Server] Ignoring ACK for unsent frame %d\n",
                       display_seq(reply.seq, window_size));
                continue;
            }

            if (!acked[reply.seq]) {
                acked[reply.seq] = 1;
                print_window(base, window_size,
                             total_frames, acked);
            } else {
                printf("[Server] Duplicate ACK %d ignored\n",
                       display_seq(reply.seq, window_size));
            }

            /*
             * The base moves only across consecutively ACKed frames.
             */
            while (base < total_frames && acked[base]) {
                base++;
            }
        } else if (reply.type == TYPE_NAK) {
            printf("[Server] Received NAK %d\n",
                   display_seq(reply.seq, window_size));

            if (sent[reply.seq] && !acked[reply.seq]) {
                printf("[Server] SR: Retransmitting ONLY Frame %d\n",
                       display_seq(reply.seq, window_size));

                if (send_data_frame(sock, reply.seq) < 0) {
                    perror("send NAK recovery");
                    return;
                }

                sent_time[reply.seq] = monotonic_ms();
            }
        }
    }

    printf("\n[Server] All %d frames acknowledged successfully!\n",
           total_frames);
}

int main(void) {
    int total_frames;
    int window_size;
    int protocol;
    int sim_type;
    int error_frame = -1;

    printf("==========================================\n");
    printf("    SLIDING WINDOW PROTOCOL SIMULATOR     \n");
    printf("==========================================\n");

    printf("Enter Total Frames to send (1-%d): ", MAX_FRAMES);

    if (scanf("%d", &total_frames) != 1 ||
        total_frames < 1 || total_frames > MAX_FRAMES) {
        fprintf(stderr, "Invalid total frame count.\n");
        return EXIT_FAILURE;
    }

    printf("Enter Window Size (1-%d): ", total_frames);

    if (scanf("%d", &window_size) != 1 ||
        window_size < 1 || window_size > total_frames) {
        fprintf(stderr, "Invalid window size.\n");
        return EXIT_FAILURE;
    }

    printf("Choose Protocol (1. Go-Back-N  2. Selective Repeat): ");

    if (scanf("%d", &protocol) != 1 ||
        (protocol != 1 && protocol != 2)) {
        fprintf(stderr, "Invalid protocol selection.\n");
        return EXIT_FAILURE;
    }

    printf("Choose Simulation Type "
           "(1. No Error  2. Data Loss  3. ACK Loss  4. NACK): ");

    if (scanf("%d", &sim_type) != 1 ||
        sim_type < 1 || sim_type > 4) {
        fprintf(stderr, "Invalid simulation type.\n");
        return EXIT_FAILURE;
    }

    if (sim_type != SCENARIO_NONE) {
        printf("Enter specific Frame Number (0 to %d): ",
               total_frames - 1);

        if (scanf("%d", &error_frame) != 1 ||
            error_frame < 0 || error_frame >= total_frames) {
            fprintf(stderr, "Invalid error frame number.\n");
            return EXIT_FAILURE;
        }
    }

    int client_fd = create_server_socket();

    if (client_fd < 0) {
        return EXIT_FAILURE;
    }

    if (send_config(client_fd, protocol, sim_type,
                    error_frame, total_frames, window_size) < 0) {
        perror("send configuration");
        close(client_fd);
        return EXIT_FAILURE;
    }

    if (protocol == 1) {
        run_gbn(client_fd, total_frames, window_size,
                sim_type, error_frame);
    } else {
        run_sr(client_fd, total_frames, window_size,
               sim_type, error_frame);
    }

    /*
     * Inform the client that the simulation is complete.
     */
    if (send_packet(client_fd, TYPE_END, -1) < 0) {
        perror("send END");
    }

    close(client_fd);
    return EXIT_SUCCESS;
}

janani@Janani:~/sem5/CN/ex5/ex5$ cat client.c
#include "protocol.h"

static int connect_to_server(void) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid server address.\n");
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Connection failed");
        close(sock);
        return -1;
    }

    return sock;
}

static int send_ack(int sock, int seq, const char *description, int window_size) {
    if (send_packet(sock, TYPE_ACK, seq) < 0) {
        perror("send ACK");
        return -1;
    }

    printf("[Client] Sent %s ACK %d\n", description,
           display_seq(seq, window_size));
    return 0;
}

static int send_nak(int sock, int seq, const char *description, int window_size) {
    if (send_packet(sock, TYPE_NAK, seq) < 0) {
        perror("send NAK");
        return -1;
    }

    printf("[Client] Sent %s NAK %d\n", description,
           display_seq(seq, window_size));
    return 0;
}

static void run_gbn_client(
    int sock,
    int total_frames,
    int window_size,
    int sim_type,
    int error_frame
) {
    int expected_seq = 0;
    int received_count = 0;
    int batch_start_seq = 0;

    int ack_loss_done = 0;
    int nack_done = 0;

    printf("--- CLIENT: GO-BACK-N ---\n");

    while (1) {
        struct Packet pkt;

        if (recv_packet(sock, &pkt) < 0) {
            printf("[Client] Connection closed.\n");
            break;
        }

        if (pkt.type == TYPE_END) {
            break;
        }

        if (pkt.type != TYPE_DATA ||
            pkt.seq < 0 || pkt.seq >= total_frames) {
            printf("[Client] Invalid DATA packet ignored.\n");
            continue;
        }

        printf("[Client] Received Frame %d\n",
               display_seq(pkt.seq, window_size));

        /*
         * Simulate one corrupted frame. The receiver discards it
         * and asks the sender to recover from the expected sequence.
         */
        if (sim_type == SCENARIO_NAK &&
            pkt.seq == error_frame && !nack_done) {

            nack_done = 1;

            printf("[Client] *** Frame %d corrupted! "
                   "Discarding frame. ***\n",
                   display_seq(pkt.seq, window_size));

            received_count = 0;

            if (send_nak(sock, expected_seq,
                         "Cumulative", window_size) < 0) {
                break;
            }

            continue;
        }

        if (pkt.seq == expected_seq) {
            if (received_count == 0) {
                batch_start_seq = pkt.seq;
            }

            printf("[Client] Accepted Frame %d in order\n",
                   display_seq(pkt.seq, window_size));

            expected_seq++;
            received_count++;

            /*
             * Custom policy: cumulative ACK every three frames,
             * or at the end of the transfer.
             */
            if (received_count == MAX_BATCH ||
                expected_seq == total_frames) {

                int cum_ack = expected_seq - 1;

                if (sim_type == SCENARIO_ACK_LOSS &&
                    !ack_loss_done &&
                    error_frame >= batch_start_seq &&
                    error_frame <= cum_ack) {

                    ack_loss_done = 1;

                    printf("[Client] *** Cumulative ACK %d "
                           "LOST IN NETWORK ***\n",
                           display_seq(cum_ack, window_size));
                } else {
                    if (send_ack(sock, cum_ack,
                                 "Cumulative", window_size) < 0) {
                        break;
                    }
                }

                received_count = 0;
            }
        } else if (pkt.seq < expected_seq) {
            /*
             * A duplicate usually means that the sender did not
             * receive an earlier ACK. Repeat the latest cumulative ACK.
             */
            printf("[Client] Duplicate Frame %d discarded\n",
                   display_seq(pkt.seq, window_size));

            if (expected_seq > 0) {
                if (send_ack(sock, expected_seq - 1,
                             "Duplicate cumulative", window_size) < 0) {
                    break;
                }
            }
        } else {
            /*
             * GBN cannot accept out-of-order frames.
             * Discard the frame and send a duplicate cumulative ACK
             * to report the last correctly received frame.
             */
            printf("[Client] Out-of-order Frame %d discarded; "
                   "expected Frame %d\n",
                   display_seq(pkt.seq, window_size),
                   display_seq(expected_seq, window_size));

            if (expected_seq > 0) {
                if (send_ack(sock, expected_seq - 1,
                             "Duplicate cumulative", window_size) < 0) {
                    break;
                }
            }
        }
    }
}

static void run_sr_client(
    int sock,
    int total_frames,
    int window_size,
    int sim_type,
    int error_frame
) {
    int received[MAX_FRAMES] = {0};

    int expected_seq = 0;
    int ack_loss_done = 0;
    int nack_done = 0;

    printf("--- CLIENT: SELECTIVE REPEAT ---\n");

    while (1) {
        struct Packet pkt;

        if (recv_packet(sock, &pkt) < 0) {
            printf("[Client] Connection closed.\n");
            break;
        }

        if (pkt.type == TYPE_END) {
            break;
        }

        if (pkt.type != TYPE_DATA ||
            pkt.seq < 0 || pkt.seq >= total_frames) {
            printf("[Client] Invalid DATA packet ignored.\n");
            continue;
        }

        int seq = pkt.seq;

        printf("[Client] Received Frame %d\n",
               display_seq(seq, window_size));

        /*
         * Simulate corruption only on the first transmission
         * of the selected frame. The retransmission is accepted.
         */
        if (sim_type == SCENARIO_NAK &&
            seq == error_frame && !nack_done) {

            nack_done = 1;

            printf("[Client] *** Frame %d corrupted! "
                   "Discarding and sending NAK. ***\n", seq);

            if (send_nak(sock, seq, "Individual", window_size) < 0) {
                break;
            }

            continue;
        }

        /*
         * Simulate losing the first ACK for the selected frame.
         * When the sender retransmits it, the ACK will be sent.
         */
        if (sim_type == SCENARIO_ACK_LOSS &&
            seq == error_frame && !ack_loss_done) {

            ack_loss_done = 1;

            printf("[Client] *** ACK %d LOST IN NETWORK ***\n",
                   display_seq(seq, window_size));

            /*
             * Do not ACK this transmission. Leave it unaccepted
             * so the retransmission can be handled normally.
             */
            continue;
        }

        /*
         * Selective Repeat accepts valid frames out of order and
         * remembers them until missing earlier frames arrive.
         */
        if (!received[seq]) {
            received[seq] = 1;

            if (seq == expected_seq) {
                printf("[Client] Accepted Frame %d in order\n",
                       display_seq(seq, window_size));
            } else {
                printf("[Client] Buffered out-of-order Frame %d\n",
                       display_seq(seq, window_size));
            }

            while (expected_seq < total_frames &&
                   received[expected_seq]) {
                expected_seq++;
            }
        } else {
            printf("[Client] Duplicate Frame %d received\n",
                   display_seq(seq, window_size));
        }

        /*
         * ACK every correctly received frame, including duplicates.
         * Re-ACKing duplicates lets the sender recover from ACK loss.
         */
        if (send_ack(sock, seq, "Individual", window_size) < 0) {
            break;
        }

        printf("[Client] Receiver next in-order frame: %d\n",
               display_seq(expected_seq, window_size));

        (void)window_size;
    }
}

int main(void) {
    int sock = connect_to_server();

    if (sock < 0) {
        return EXIT_FAILURE;
    }

    struct Config cfg;

    if (recv_all(sock, &cfg, sizeof(cfg)) < 0) {
        fprintf(stderr, "Failed to receive configuration.\n");
        close(sock);
        return EXIT_FAILURE;
    }

    if ((cfg.protocol != 1 && cfg.protocol != 2) ||
        cfg.total_frames < 1 ||
        cfg.total_frames > MAX_FRAMES ||
        cfg.window_size < 1 ||
        cfg.window_size > cfg.total_frames ||
        cfg.sim_type < 1 ||
        cfg.sim_type > 4) {

        fprintf(stderr, "Invalid server configuration.\n");
        close(sock);
        return EXIT_FAILURE;
    }

    printf("\nConnected to Server.\n");
    printf("Protocol: %s\n",
           cfg.protocol == 1 ? "Go-Back-N" : "Selective Repeat");

    printf("Scenario: %d\n", cfg.sim_type);
    printf("Target Error Frame: %d\n", cfg.error_frame);
    printf("Total Frames: %d\n", cfg.total_frames);
    printf("Window Size: %d\n\n", cfg.window_size);

    if (cfg.protocol == 1) {
        run_gbn_client(sock, cfg.total_frames, cfg.window_size,
                       cfg.sim_type, cfg.error_frame);
    } else {
        run_sr_client(sock, cfg.total_frames, cfg.window_size,
                      cfg.sim_type, cfg.error_frame);
    }

    printf("\n[Client] Transfer session ended.\n");

    close(sock);
    return EXIT_SUCCESS;
}

janani@Janani:~/sem5/CN/ex5/ex5$ cat protocol.h
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/time.h>

#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8080
#define MAX_FRAMES 200
#define MAX_BATCH 3

#define GBN_TIMEOUT_MS 3000
#define SR_TIMEOUT_MS  1000

#define TYPE_END 0
#define TYPE_DATA 1
#define TYPE_ACK 2
#define TYPE_NAK 3

#define SCENARIO_NONE     1
#define SCENARIO_DATA_LOSS 2
#define SCENARIO_ACK_LOSS  3
#define SCENARIO_NAK       4

struct Packet {
    int32_t type;
    int32_t seq;
};

struct Config {
    int32_t protocol;
    int32_t sim_type;
    int32_t error_frame;
    int32_t total_frames;
    int32_t window_size;
};

/* Send all requested bytes, handling partial writes. */
static inline int send_all(int sock, const void *buffer, size_t length) {
    const char *ptr = (const char *)buffer;
    size_t sent = 0;

    while (sent < length) {
        ssize_t n = send(sock, ptr + sent, length - sent, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (n == 0) {
            return -1;
        }

        sent += (size_t)n;
    }

    return 0;
}

/*
 * Receive exactly the requested number of bytes.
 * Returns:
 *   0  = success
 *  -1  = connection closed or an error occurred
 */
static inline int recv_all(int sock, void *buffer, size_t length) {
    char *ptr = (char *)buffer;
    size_t received = 0;

    while (received < length) {
        ssize_t n = recv(sock, ptr + received, length - received, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (n == 0) {
            return -1;
        }

        received += (size_t)n;
    }

    return 0;
}

static inline int send_packet(int sock, int type, int seq) {
    struct Packet pkt;

    pkt.type = type;
    pkt.seq = seq;

    return send_all(sock, &pkt, sizeof(pkt));
}

static inline int recv_packet(int sock, struct Packet *pkt) {
    if (recv_all(sock, pkt, sizeof(*pkt)) < 0) {
        return -1;
    }

    if (pkt->type < TYPE_END || pkt->type > TYPE_NAK) {
        return -1;
    }

    return 0;
}

/* Monotonic clock: unaffected by changes to the system clock. */
static inline int64_t monotonic_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }

    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/*
 * Wait for socket readability.
 * Returns:
 *   1 = readable
 *   0 = timeout
 *  -1 = error
 */
static inline int wait_for_data_ms(int sock, int timeout_ms) {
    fd_set set;
    struct timeval tv;

    FD_ZERO(&set);
    FD_SET(sock, &set);

    if (timeout_ms < 0) {
        timeout_ms = 0;
    }

    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    int result = select(sock + 1, &set, NULL, NULL, &tv);

    if (result < 0 && errno == EINTR) {
        return 0;
    }

    return result;
}

static inline int display_seq(int seq, int window_size) {
    if (seq < 0) return seq;
    return seq % (window_size + 1);
}

static inline void print_window(
    int base,
    int window_size,
    int total_frames,
    const int acked[]
) {
    printf("\nSliding Window: [ ");

    int end = base + window_size;

    if (end > total_frames) {
        end = total_frames;
    }

    for (int i = base; i < end; i++) {
        if (acked[i]) {
            printf("(ACK %d) ", display_seq(i, window_size));
        } else {
            printf("%d ", display_seq(i, window_size));
        }
    }

    printf("]\n");
    fflush(stdout);
}

#endif
