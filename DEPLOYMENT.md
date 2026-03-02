# DeepCon SOIF 대시보드 — 배포 가이드

**GitHub 레포**: [https://github.com/tony-tjlabs/deepen_soif](https://github.com/tony-tjlabs/deepen_soif)  
**배포 대상**: Streamlit Cloud (또는 로컬/서버 직접 실행)

---

## 📋 배포 전 체크리스트

- [ ] 이 폴더(Release/DeepCon_SOIF)에 배포용 파일만 포함됨 (main.py, src/, cache/, requirements.txt, .streamlit/config.toml, Datafile/ssmp_structure)
- [ ] 비밀번호·API 키는 코드/저장소에 없음 → **Streamlit Cloud Secrets**에서만 설정
- [ ] `cache/` 에 processed_*.parquet, analytics_* 파일 있음 (대시보드가 캐시만 읽음)

---

## 🚀 방법 1: Streamlit Cloud 배포 (권장)

### 1단계: GitHub에 코드 푸시

```bash
# Release 폴더로 이동
cd /Users/Tony_mac/Desktop/Release/DeepCon_SOIF

# Git 초기화 (이미 되어 있으면 생략)
git init
git remote add origin https://github.com/tony-tjlabs/deepen_soif.git

# 전체 추가 (secrets.toml, .env 는 이 폴더에 없으므로 안전)
git add .
git commit -m "Deploy DeepCon SOIF dashboard"
git branch -M main
git push -u origin main
```

> ⚠️ **주의**: `.env`, `.streamlit/secrets.toml` 은 이 배포 폴더에 포함하지 않음. 비밀번호는 Streamlit Cloud Secrets에만 입력.

### 2단계: Streamlit Cloud에서 앱 생성

1. [share.streamlit.io](https://share.streamlit.io) 로그인 (GitHub 연동)
2. **New app** → Repository: `tony-tjlabs/deepen_soif`, Branch: `main`, Main file path: `main.py`
3. **Advanced settings** → Secrets에 아래 추가:

```toml
APP_PASSWORD = "wonderful2$"
CLOUD_MODE = "true"
ANTHROPIC_API_KEY = "sk-ant-..."
```

4. **Deploy** 클릭

### 3단계: 배포 후 확인

- 접속 URL로 들어가서 비밀번호 입력 후 대시보드 로드 확인
- 사이드바에서 **CLOUD_MODE** 로 Admin 메뉴가 숨겨진 것 확인
- (선택) AI 해석 사용 시 Claude API 동작 확인

---

## 🖥️ 방법 2: 로컬/서버 직접 실행

```bash
cd /Users/Tony_mac/Desktop/Release/DeepCon_SOIF

# 비밀번호 설정 (로컬)
# .streamlit/secrets.toml 생성 후:
#   APP_PASSWORD = "wonderful2$"

# 의존성 설치
pip install -r requirements.txt

# 실행
streamlit run main.py --server.port 8501
```

---

## 🔒 보안 (TJLABS 배포 원칙)

1. **비밀번호**: 코드/저장소에 두지 않음. Streamlit Cloud는 **Secrets**, 로컬은 **.streamlit/secrets.toml** (git 제외)
2. **API 키**: ANTHROPIC_API_KEY 도 Secrets 또는 .env
3. **HTTPS**: Streamlit Cloud는 기본 HTTPS 제공
4. **CLOUD_MODE**: Cloud에서는 `true` 로 두어 Pipeline/Admin 메뉴 비노출

---

## 🔄 업데이트 절차

1. 개발은 `TJLABS_Research/Project/SKEP/DeepCon_SOIF` 에서 진행
2. 배포 시 **Release/DeepCon_SOIF** 에 필요한 파일만 다시 복사
3. `cd /Users/Tony_mac/Desktop/Release/DeepCon_SOIF` 후:
   ```bash
   git add .
   git commit -m "Update: ..."
   git push origin main
   ```
4. Streamlit Cloud는 main 브랜치 푸시 시 자동 재배포

---

**레포**: [tony-tjlabs/deepen_soif](https://github.com/tony-tjlabs/deepen_soif)  
**관리**: TJLABS
