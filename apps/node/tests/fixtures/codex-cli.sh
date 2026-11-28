#!/bin/sh
set -eu

[ "${0##*/}" = codex ]

if [ "$#" -eq 1 ] && [ "$1" = --help ]; then
    if [ "${COOPLET_FIXTURE_MODE:-}" != no-remote ]; then
        printf '    --remote <ADDR>\n'
    fi
    exit 0
fi

[ "$1" = app-server ]
shift
case "$1" in
    --help)
        printf '    --listen <URL>\n'
        ;;
    --stdio)
        [ -d "$CODEX_HOME" ]
        IFS= read -r request
        case "$request" in *initialize*) ;; *) exit 1 ;; esac
        if [ "${COOPLET_FIXTURE_MODE:-}" = probe-timeout ]; then
            command -p sleep 30
        else
            printf '%s\n' '{"id":1,"result":{"codexHome":"/fixture","userAgent":"codex-fixture/99.0.0"}}'
        fi
        ;;
    generate-json-schema)
        [ "${COOPLET_FIXTURE_MODE:-}" != no-schema ]
        [ "$2" = --out ]
        command -p mkdir -p "$3/v2"
        printf '%s\n' '{"definitions":{"TurnToolOutput":{"properties":{"namespace":{},"name":{},"output":{},"retain":{}}}},"properties":{"toolOutput":{},"outputSchema":{}}}' > "$3/v2/TurnStartParams.json"
        ;;
    --listen)
        touch "$COOPLET_FIXTURE_ROOT/managed-spawned"
        export COOPLET_FIXTURE_SOCKET="${2#unix://}"
        shift 2
        while [ "$#" -gt 0 ]; do
            [ "$1" = -c ]
            case "$2" in
                mcp_servers.cooplet.command=*) export COOPLET_FIXTURE_MCP_COMMAND="${2#*=}" ;;
                mcp_servers.cooplet.args=*) export COOPLET_FIXTURE_MCP_ARGS="${2#*=}" ;;
                mcp_servers.cooplet.required=true) export COOPLET_FIXTURE_MCP_REQUIRED=1 ;;
            esac
            shift 2
        done
        if [ "${COOPLET_FIXTURE_MODE:-}" = foreign-peer ]; then
            ln -s "$COOPLET_FIXTURE_FOREIGN" "$COOPLET_FIXTURE_SOCKET"
            sleep 30
        else
            # Like the npm launcher, the WS server has a different PID in the same group.
            "$COOPLET_FIXTURE_SELF" --exact app_server_fixture --nocapture --quiet &
            wait "$!"
        fi
        ;;
    *)
        exit 2
        ;;
esac
