/*
  Rodar:
  g++ -fopenmp reader.cpp -o omp
  ./omp

  Feito por:
  - Luis Durante
  - Guilherme Rabelo
  - Victor Moreno
*/
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>

class CsvValue
{
public:
  int count = 1;
  std::string value;
  int id = 0;
};

class CsvHeader
{
public:
  std::vector<CsvValue> fields;
  std::string name;
};

// function declaration:
void printResult(std::vector<CsvHeader>, std::vector<std::string>);
void write_csv(std::string, std::string, CsvHeader);

int main()
{
  int maxLines = 0;
  int option = 0;
  std::string dsName = "";
  while (option == 0) {
    std::cout << "*Leitura de CSV*" << "\n\n";
    std::cout << "1 - Digitar o nome do arquivo" << "\n";
    std::cout << "2 - Escolher a quantidade de linhas (padrao arquivo inteiro)" << "\n";
    std::cout << "3 - Iniciar" << "\n";
    std::cout << "4 - Sair" << "\n";
    std::cout << "Opção: ";
    std::cin >> option;

    if (option == 1) {
      std::cout << "Digite o nome do arquivo com extensão: ";
      std::cin >> dsName;
      option = 0;
    }

    if (option == 2) {
      std::cout << "Escolha a quantidade de linhas que serão lidas " << "\n" 
        << "- Se o número for menor ou igual a zero, todas as linhas serão lidas (Essa opção pode demorar dependendo do tamanho do arquivo)\n" 
        << "Linhas: ";
      std::cin >> maxLines;
      option = 0;
    }

    if (option == 3) {
      option = 3;
    }

    if (option == 4) {
      return 0;
    }

  }



  std::ifstream file(dsName);
  std::string line;
  std::vector<std::vector<std::string>> lines;
  std::vector<std::string> headers;
  int categoricColumns[] = {1, 2, 3, 5, 6, 7, 8, 17, 18, 19, 20, 24};

  std::vector<CsvHeader> results;
  std::vector<std::vector<std::string>> results_names;
  results_names.resize(27);
  results.resize(27);

  int lineCount = 0;
  int isHeader = 1;

  std::ofstream codedCsv("resultados-codificados.csv");

  if (file)
  {
    while (std::getline(file, line))
    {
      size_t n = lines.size();
      lines.resize(n + 1);

      std::istringstream ss(line);
      std::string field, push_field("");
      bool no_quotes = true;

      while (std::getline(ss, field, ','))
      {
        if (static_cast<size_t>(std::count(field.begin(), field.end(), '"')) % 2 != 0)
        {
          no_quotes = !no_quotes;
        }

        push_field += field + (no_quotes ? "" : ",");

        if (no_quotes)
        {
          lines[n].push_back(push_field);
          push_field.clear();
        }
      }

      if (isHeader == 1)
      {
        headers.swap(lines[0]);
        std::vector<std::vector<std::string>>().swap(lines);
        isHeader = 0;
        for (int i = 0; i < headers.size(); i++)
        {
          bool exists = std::find(std::begin(categoricColumns),
                                  std::end(categoricColumns), i) != std::end(categoricColumns);
          if (exists)
          {
            codedCsv << headers[i];

            if (i + 1 != headers.size() && i != 24)
            {
              codedCsv << ",";
            }
          }
        }
        codedCsv << "\n";
        continue;
      }
      lineCount++;
      std::cout << "Linha - " << lineCount << "\n";

      #pragma omp parallel
      {
        #pragma omp sections
        {
          #pragma omp section
            for (int i = 0; i < lines[n].size(); i++)
            {
              // verifica se é uma coluna que a gente quer - TODO -> Passar pra array
              bool exists = std::find(std::begin(categoricColumns),
                                      std::end(categoricColumns), i) != std::end(categoricColumns);
              if (exists)
              {

                // Se o size for 0 é pq nenhum foi inserido ainda, logo, insere ele e passa
                if (results[i].fields.size() == 0)
                {
                  CsvValue field;
                  field.value = lines[n][i];
                  field.id = results[i].fields.size();
                  results_names[i].push_back(lines[n][i]);
                  results[i].fields.push_back(field);
                  continue;
                }

                //ve se o nome existe no array de nomes
                auto it = std::find(results_names[i].begin(), results_names[i].end(), lines[n][i]);
                //Se achou, soma + 1 e passa, se não, cria.
                if (it == results_names[i].end())
                {
                  CsvValue field;
                  field.value = lines[n][i];
                  field.id = results[i].fields.size();
                  results_names[i].push_back(lines[n][i]);
                  results[i].fields.push_back(field);
                }
                else
                {
                  auto index = std::distance(results_names[i].begin(), it);
                  results[i].fields[index].count++;
                }
              }
            }
          #pragma omp section
            for (int x = 0; x < lines[n].size(); x++)
            {
              bool exists = std::find(std::begin(categoricColumns),
                                      std::end(categoricColumns), x) != std::end(categoricColumns);
              if (exists)
              {
                bool achou = false;
                int j = 0;
                int id;
                //Procura o objeto no vetor pelo nome
                while (!achou)
                {
                  for (j = 0; j < results[x].fields.size(); j++)
                  {
                    if (results[x].fields[j].value == lines[n][x])
                    {
                      achou = true;
                      break;
                    }
                  }
                }

                if (achou)
                {
                  codedCsv << results[x].fields[j].id;

                  if (x + 1 != lines[n].size() && x != 24)
                  {
                    codedCsv << ",";
                  }
                }
              }

              if (x + 1 == lines[n].size())
              {
                codedCsv << "\n";
              }
            }
        }
      }
      //limpa vetor da memoria
      std::vector<std::vector<std::string>>().swap(lines);

      if (lineCount > 0 && lineCount == maxLines)
      {
        break;
      }
    }
  }
  else {
    std::cout << "Falha ao abrir arquivo - verifique o nome" << "\n";
    codedCsv.close();
    return 0;
  }
  file.close();
  codedCsv.close();
  for (int i = 0; i < results.size(); i++)
  {
    if (results[i].fields.size() < 1)
    {
      continue;
    }

    write_csv(headers[i] + ".csv", headers[i], results[i]);
  }

  std::cout << "Arquivos gravados" << "\n";

  return 0;
}

void write_csv(std::string filename, std::string colname, CsvHeader results)
{
  std::ofstream myFile(filename);

  myFile << colname << ",count,id"
         << "\n";

  for (int j = 0; j < results.fields.size(); j++)
  {
    myFile << results.fields[j].value << ",";
    myFile << results.fields[j].count << ",";
    myFile << results.fields[j].id << "\n";
  }

  myFile.close();
}