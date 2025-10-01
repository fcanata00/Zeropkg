# zeropkg1.0/plugins/sandbox_bwrap.py
"""
Sandbox usando bubblewrap (bwrap).

Exporta a classe BwrapSandbox com método `run(cmd, workdir, env, log_file, quiet, timeout)`.
- cmd: list[str] ou str (se str, será executado via shell -c)
- workdir: diretório dentro do qual o comando será executado (bindado)
- env: dict de variáveis de ambiente (opcional)
- log_file: file-like object aberto para escrita (obrigatório para gravação de logs)
- quiet: se True, suprime saída detalhada e apenas escreve sumário no stdout
- timeout: segundos para timeout (opcional)

Requisitos:
- bubblewrap (bwrap) instalado no sistema
- se rodar sem user namespaces, precisa de permissões adequadas (root)
"""
import shutil
import subprocess
import os
import shlex
from datetime import datetime
from typing import Optional, Union, Sequence

BWRAP_BIN = shutil.which("bwrap")

class BwrapUnavailable(Exception):
    pass

class BwrapSandbox:
    def __init__(self, bwrap_path: Optional[str] = None, debug: bool = False):
        self.bwrap = bwrap_path or BWRAP_BIN
        self.debug = bool(debug)
        if not self.bwrap:
            raise BwrapUnavailable("bubblewrap (bwrap) não encontrado no PATH. Instale 'bubblewrap'.")

    def _timestamp(self):
        return datetime.now().isoformat(sep=" ", timespec="seconds")

    def _log(self, log_file, msg: str):
        # escreve no arquivo de log com timestamp
        if log_file:
            log_file.write(f"[{self._timestamp()}] {msg}\n")
            log_file.flush()

    def _build_bwrap_cmd(self,
                         workdir: str,
                         env: Optional[dict] = None,
                         readonly_bind_paths: Optional[Sequence[str]] = None,
                         ro_bind_host_to_guest: Optional[Sequence[tuple]] = None,
                         use_tmpfs_for: Optional[Sequence[str]] = None,
                         keep_dev: bool = True) -> list:
        """
        Monta a lista de argumentos para bwrap.
        - workdir será bindado e será o diretório de trabalho (--chdir)
        - readonly_bind_paths: lista de host paths para montar como read-only no mesmo caminho (ex: ['/usr'])
        - ro_bind_host_to_guest: lista de tuples (host_path, guest_path) para ro-bind
        - use_tmpfs_for: lista de guest paths que serão tmpfs (ex: ['/tmp'])
        - keep_dev: se True, faz --dev /dev
        """

        cmd = [self.bwrap, "--unshare-all", "--die-with-parent"]

        # bind do workdir (leitura/escrita)
        workdir = os.path.abspath(workdir)
        cmd += ["--bind", workdir, workdir]

        # montar /proc e /sys e /dev de maneira controlada
        cmd += ["--proc", "/proc"]

        if keep_dev:
            cmd += ["--dev", "/dev"]
        else:
            # cria /dev vazio para maior isolamento
            cmd += ["--tmpfs", "/dev"]

        # tmpfs em /tmp e /var/tmp
        if use_tmpfs_for is None:
            use_tmpfs_for = ["/tmp", "/var/tmp"]
        for p in use_tmpfs_for:
            cmd += ["--tmpfs", p]

        # mounts read-only de caminhos comuns do sistema: /usr, /bin, /lib, /lib64
        default_ro = readonly_bind_paths or ["/usr", "/bin", "/lib", "/lib64", "/sbin", "/etc"]
        for p in default_ro:
            if os.path.exists(p):
                cmd += ["--ro-bind", p, p]

        # adicionais: ro-bind host->guest
        if ro_bind_host_to_guest:
            for host, guest in ro_bind_host_to_guest:
                if os.path.exists(host):
                    cmd += ["--ro-bind", host, guest]

        # setar diretório de trabalho
        cmd += ["--chdir", workdir]

        # propagar algumas env vars via --setenv
        if env:
            # ensure PATH exists if not given
            if "PATH" not in env:
                env = dict(env)
                env["PATH"] = os.environ.get("PATH", "/usr/bin:/bin")
            # pass only safe env vars explicitly
            for k, v in env.items():
                # bwrap espera valores simples, evitar None
                if v is None:
                    continue
                cmd += ["--setenv", str(k), str(v)]

        # tornar a rede isolada (unshare-all já faz isso), mas permitir --share-net se necessário (não exposto aqui)
        # fim de cmd; o comando a executar será acrescentado
        return cmd

    def run(self,
            cmd: Union[str, Sequence[str]],
            workdir: str,
            env: Optional[dict] = None,
            log_file = None,
            quiet: bool = False,
            timeout: Optional[int] = None) -> int:
        """
        Executa `cmd` dentro do bubblewrap sandbox.
        - Se cmd for string, será executado por `sh -c 'cmd'`.
        - A saída (stdout+stderr) será streamada para log_file. Se quiet=False, também será impressa no stdout em tempo real.
        - Retorna o código de saída do processo.
        """
        if not self.bwrap:
            raise BwrapUnavailable("bwrap não disponível")

        if isinstance(cmd, str):
            exec_cmd = ["/bin/sh", "-c", cmd]
        else:
            exec_cmd = list(cmd)

        # construir cmd bwrap
        bwrap_cmd = self._build_bwrap_cmd(workdir=workdir, env=env)

        # colocar o comando a executar (mantemos shell invocation para facilitar env expansions)
        bwrap_cmd += ["--"] + exec_cmd

        # log header
        header = f"[SANDBOX START] workdir={workdir} cmd={' '.join(shlex.quote(x) for x in exec_cmd)}"
        self._log(log_file, header)
        if not quiet:
            print(header)

        # debug print do bwrap command (muito verboso) — opcional
        if self.debug:
            dbg = "[BWRAP CMD] " + " ".join(shlex.quote(x) for x in bwrap_cmd)
            self._log(log_file, dbg)
            if not quiet:
                print(dbg)

        # executar e streamar a saída
        try:
            proc = subprocess.Popen(
                bwrap_cmd,
                cwd=workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=None  # env is passed into bwrap via --setenv; do not pass here
            )
        except FileNotFoundError as e:
            self._log(log_file, f"falha ao iniciar bwrap: {e}")
            raise

        try:
            # ler e encaminhar linhas
            while True:
                line = proc.stdout.readline()
                if line == "" and proc.poll() is not None:
                    break
                if line:
                    if log_file:
                        log_file.write(line)
                        log_file.flush()
                    if not quiet:
                        # imprimir em tempo real
                        print(line, end="")
            return_code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            self._log(log_file, f"[SANDBOX TIMEOUT] matando processo após {timeout}s")
            if not quiet:
                print(f"[SANDBOX TIMEOUT] comando excedeu {timeout}s e foi terminado")
            return_code = -1
        except Exception as e:
            proc.kill()
            self._log(log_file, f"[SANDBOX ERROR] {e}")
            raise
        finally:
            footer = f"[SANDBOX END] exit={return_code}"
            self._log(log_file, footer)
            if not quiet:
                print(footer)

        return return_code
